"""Compare boundaries_nlsc and boundaries_osm using PostGIS spatial metrics.

Matches rows via NLSC VILLCODE <-> OSM nat_ref, then computes per-pair:
  - IoU (Intersection over Union)
  - Area difference (OSM area - NLSC area, in m²)
  - Hausdorff distance (in metres)
  - Individual areas in m²

Unmatched NLSC villages (no OSM nat_ref) and orphan OSM relations
(no matching NLSC VILLCODE) are included with a 'status' column.

Output: compare.csv
"""

import csv
import os

import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
OUTPUT_FILE = "compare.csv"

# All geometry operations are done in EPSG:3826 (TWD97 TM2 zone 121) so that
# area and distance results are in metres / m².
QUERY = """
WITH
nlsc AS (
    SELECT
        properties->>'VILLCODE'   AS villcode,
        properties->>'COUNTYNAME' AS county,
        properties->>'TOWNNAME'   AS town,
        properties->>'VILLNAME'   AS village,
        ST_Transform(geom, 3826)  AS geom
    FROM boundaries_nlsc
    WHERE properties->>'VILLCODE' IS NOT NULL
),
osm AS (
    SELECT
        properties->>'nat_ref'          AS nat_ref,
        (properties->>'osm_id')::bigint AS osm_id,
        properties->>'name'             AS osm_name,
        ST_Transform(geom, 3826)        AS geom
    FROM boundaries_osm
),
matched AS (
    SELECT
        'matched'        AS status,
        n.county,
        n.town,
        n.village,
        o.osm_id,
        ROUND(ST_Area(n.geom)::numeric, 2)                                AS area_nlsc_m2,
        ROUND(ST_Area(o.geom)::numeric, 2)                                AS area_osm_m2,
        ROUND((ST_Area(o.geom) - ST_Area(n.geom))::numeric, 2)           AS area_diff_m2,
        CASE
            WHEN ST_Area(ST_Union(n.geom, o.geom)) = 0 THEN NULL
            ELSE ROUND(
                (ST_Area(ST_Intersection(n.geom, o.geom))
                 / ST_Area(ST_Union(n.geom, o.geom)))::numeric, 6)
        END                                                                AS iou,
        ROUND(ST_HausdorffDistance(n.geom, o.geom)::numeric, 2)          AS hausdorff_m
    FROM nlsc n
    JOIN osm o ON o.nat_ref = n.villcode
),
nlsc_only AS (
    SELECT
        'nlsc_only'      AS status,
        n.county,
        n.town,
        n.village,
        NULL::bigint                                                       AS osm_id,
        ROUND(ST_Area(n.geom)::numeric, 2)                                AS area_nlsc_m2,
        NULL::numeric                                                      AS area_osm_m2,
        NULL::numeric                                                      AS area_diff_m2,
        NULL::numeric                                                      AS iou,
        NULL::numeric                                                      AS hausdorff_m
    FROM nlsc n
    WHERE NOT EXISTS (SELECT 1 FROM osm o WHERE o.nat_ref = n.villcode)
),
osm_only AS (
    SELECT
        'osm_only'       AS status,
        NULL::text                                                         AS county,
        NULL::text                                                         AS town,
        o.osm_name                                                        AS village,
        o.osm_id,
        NULL::numeric                                                      AS area_nlsc_m2,
        ROUND(ST_Area(o.geom)::numeric, 2)                                AS area_osm_m2,
        NULL::numeric                                                      AS area_diff_m2,
        NULL::numeric                                                      AS iou,
        NULL::numeric                                                      AS hausdorff_m
    FROM osm o
    WHERE NOT EXISTS (SELECT 1 FROM nlsc n WHERE n.villcode = o.nat_ref)
)
SELECT * FROM matched
UNION ALL
SELECT * FROM nlsc_only
UNION ALL
SELECT * FROM osm_only
ORDER BY status, county, town, village;
"""

FIELDNAMES = [
    "status",
    "county",
    "town",
    "village",
    "osm_id",
    "area_nlsc_m2",
    "area_osm_m2",
    "area_diff_m2",
    "iou",
    "hausdorff_m",
]


def main():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            rows = cur.fetchall()
    finally:
        conn.close()

    counts = {"matched": 0, "nlsc_only": 0, "osm_only": 0}
    for row in rows:
        counts[row[0]] += 1

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDNAMES)
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_FILE}")
    print(f"  matched:   {counts['matched']}")
    print(f"  nlsc_only: {counts['nlsc_only']}")
    print(f"  osm_only:  {counts['osm_only']}")

    iou_vals = [float(r[8]) for r in rows if r[0] == "matched" and r[8] is not None]
    if iou_vals:
        print(f"IoU — min: {min(iou_vals):.4f}  avg: {sum(iou_vals)/len(iou_vals):.4f}  max: {max(iou_vals):.4f}")


if __name__ == "__main__":
    main()
