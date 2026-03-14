"""Import Taiwan village boundaries from OpenStreetMap into boundaries_osm table.

Fetches all nested relations with boundary=administrative and admin_level=9
within OSM relation 449220 (Taiwan) via Overpass API.
"""

import os
import time

import psycopg2
from psycopg2.extras import Json
import requests
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid

DATABASE_URL = os.environ["DATABASE_URL"]
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# area(3600449220) converts relation 449220 to an Overpass area for spatial filter.
OVERPASS_QUERY = """
[out:json][timeout:900];
area(3600449220)->.taiwan;
rel(area.taiwan)[boundary=administrative][admin_level=9];
out geom;
"""


def assemble_rings(ways: list[list[tuple[float, float]]]) -> list[list[tuple[float, float]]]:
    """Assemble a list of ways (coordinate sequences) into closed rings.

    Ways may be given in any order and may need to be reversed to connect.
    """
    rings = []
    remaining = [list(w) for w in ways]

    while remaining:
        ring = remaining.pop(0)
        changed = True
        while changed and ring[0] != ring[-1]:
            changed = False
            for i, way in enumerate(remaining):
                if way[0] == ring[-1]:
                    ring = ring + way[1:]
                    remaining.pop(i)
                    changed = True
                    break
                elif way[-1] == ring[-1]:
                    ring = ring + list(reversed(way))[1:]
                    remaining.pop(i)
                    changed = True
                    break
                elif way[0] == ring[0]:
                    ring = list(reversed(way)) + ring[1:]
                    remaining.pop(i)
                    changed = True
                    break
                elif way[-1] == ring[0]:
                    ring = way + ring[1:]
                    remaining.pop(i)
                    changed = True
                    break

        if len(ring) >= 4 and ring[0] == ring[-1]:
            rings.append(ring)
        else:
            print(f"  Warning: unclosed ring with {len(ring)} nodes, skipping")

    return rings


def make_polygon(ring: list[tuple[float, float]], holes: list) -> Polygon | None:
    """Build a valid Polygon from an exterior ring and optional holes."""
    try:
        poly = Polygon(ring, holes)
        if not poly.is_valid:
            poly = make_valid(poly)
        return poly
    except Exception as exc:
        print(f"  Warning: failed to build polygon: {exc}")
        return None


def build_multipolygon(members: list[dict]):
    """Build a Shapely geometry from OSM relation members (with inline geometry)."""
    outer_ways: list[list[tuple[float, float]]] = []
    inner_ways: list[list[tuple[float, float]]] = []

    for member in members:
        if member["type"] != "way":
            continue
        raw = member.get("geometry", [])
        coords = [(pt["lon"], pt["lat"]) for pt in raw]
        if len(coords) < 2:
            continue
        if member.get("role") == "inner":
            inner_ways.append(coords)
        else:
            outer_ways.append(coords)

    outer_rings = assemble_rings(outer_ways)
    inner_rings = assemble_rings(inner_ways)

    if not outer_rings:
        return None

    inner_polys = []
    for ring in inner_rings:
        poly = make_polygon(ring, [])
        if poly is not None:
            inner_polys.append(poly)

    result_polys = []
    for ring in outer_rings:
        outer = make_polygon(ring, [])
        if outer is None:
            continue
        holes = [
            list(inner.exterior.coords)
            for inner in inner_polys
            if outer.contains(inner)
        ]
        poly = make_polygon(ring, holes)
        if poly is not None:
            result_polys.append(poly)

    if not result_polys:
        return None
    if len(result_polys) == 1:
        return result_polys[0]
    return MultiPolygon(result_polys)


def overpass_fetch(query: str, retries: int = 5, backoff: float = 60.0) -> dict:
    """POST an Overpass query, retrying with exponential back-off on failure."""
    for attempt in range(1, retries + 1):
        try:
            print(f"  Attempt {attempt}/{retries} ...")
            resp = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=960,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            print(f"  Failed: {exc}")
            if attempt == retries:
                raise
            wait = backoff * attempt
            print(f"  Retrying in {wait:.0f}s ...")
            time.sleep(wait)


def main():
    print("Querying Overpass API for OSM admin_level=9 boundaries in Taiwan...")
    data = overpass_fetch(OVERPASS_QUERY)

    relations = [el for el in data["elements"] if el["type"] == "relation"]
    print(f"Found {len(relations)} admin_level=9 relations")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS boundaries_osm")
            cur.execute(
                """
                CREATE TABLE boundaries_osm (
                    id SERIAL PRIMARY KEY,
                    geom geometry(Geometry, 4326),
                    properties jsonb
                )
                """
            )
            cur.execute("CREATE INDEX ON boundaries_osm USING GIST (geom)")

            inserted = 0
            skipped = 0
            for rel in relations:
                geom = build_multipolygon(rel.get("members", []))
                if geom is None:
                    print(f"  Warning: no geometry for relation {rel['id']}, skipping")
                    skipped += 1
                    continue

                tags = rel.get("tags", {})
                props = {"osm_id": rel["id"], **tags}

                cur.execute(
                    "INSERT INTO boundaries_osm (geom, properties) "
                    "VALUES (ST_GeomFromText(%s, 4326), %s)",
                    (geom.wkt, Json(props)),
                )
                inserted += 1

        conn.commit()
        print(f"Inserted {inserted} features into boundaries_osm ({skipped} skipped)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
