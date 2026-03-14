"""Import Taiwan village boundaries from NLSC into boundaries_nlsc table."""

import io
import json
import os
import zipfile
import tempfile

import geopandas as gpd
import psycopg2
from psycopg2.extras import Json
import requests

NLSC_URL = (
    "https://maps.nlsc.gov.tw/download/"
    "%E6%9D%91(%E9%87%8C)%E7%95%8C(TWD97%E7%B6%93%E7%B7%AF%E5%BA%A6).zip"
)
DATABASE_URL = os.environ["DATABASE_URL"]


def to_serializable(val):
    """Convert a value to a JSON-serializable Python type."""
    import math
    import numpy as np
    import pandas as pd

    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return None if math.isnan(float(val)) else float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    if isinstance(val, (int, float, str, bool)):
        return val
    return str(val)


def main():
    print("Downloading NLSC village boundaries...")
    # NLSC server has a defective certificate (missing Subject Key Identifier);
    # disable verification and suppress the resulting urllib3 warning.
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    resp = requests.get(NLSC_URL, timeout=180, verify=False)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content):,} bytes")

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            zf.extractall(tmpdir)

        shp_files = []
        for root, _dirs, files in os.walk(tmpdir):
            for f in files:
                if f.lower().endswith(".shp"):
                    path = os.path.join(root, f)
                    shp_files.append((os.path.getsize(path), path))

        if not shp_files:
            raise RuntimeError("No SHP files found in ZIP")

        shp_files.sort(reverse=True)
        size, shp_path = shp_files[0]
        print(f"Using SHP: {os.path.basename(shp_path)} ({size:,} bytes)")

        gdf = gpd.read_file(shp_path, engine="pyogrio")
        print(f"CRS: {gdf.crs}, features: {len(gdf)}")

        if gdf.crs is None or str(gdf.crs).upper() in ("", "NONE"):
            # Fallback: assume TWD97 geographic (EPSG:3824)
            gdf = gdf.set_crs("EPSG:3824")

        gdf = gdf.to_crs("EPSG:4326")

        conn = psycopg2.connect(DATABASE_URL)
        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS boundaries_nlsc")
                cur.execute(
                    """
                    CREATE TABLE boundaries_nlsc (
                        id SERIAL PRIMARY KEY,
                        geom geometry(Geometry, 4326),
                        properties jsonb
                    )
                    """
                )
                cur.execute("CREATE INDEX ON boundaries_nlsc USING GIST (geom)")

                for _, row in gdf.iterrows():
                    geom = row.geometry
                    if geom is None or geom.is_empty:
                        continue
                    props = {
                        k: to_serializable(v)
                        for k, v in row.items()
                        if k != "geometry"
                    }
                    cur.execute(
                        "INSERT INTO boundaries_nlsc (geom, properties) "
                        "VALUES (ST_GeomFromText(%s, 4326), %s)",
                        (geom.wkt, Json(props)),
                    )

            conn.commit()
            print(f"Inserted {len(gdf)} features into boundaries_nlsc")
        finally:
            conn.close()


if __name__ == "__main__":
    main()
