"""Compare boundaries_nlsc and boundaries_osm using PostGIS spatial metrics.

Matches rows via NLSC VILLCODE <-> OSM nat_ref, then computes per-pair:
  - IoU (Intersection over Union)
  - Area difference (OSM area - NLSC area, in m²)
  - Hausdorff distance (in metres)
  - Individual areas in m²

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
        properties->>'nat_ref'   AS nat_ref,
        (properties->>'osm_id')::bigint AS osm_id,
        ST_Transform(geom, 3826) AS geom
    FROM boundaries_osm
    WHERE properties->>'nat_ref' IS NOT NULL
),
pairs AS (
    SELECT
        n.county,
        n.town,
        n.village,
        o.osm_id,
        n.geom AS ng,
        o.geom AS og
    FROM nlsc n
    JOIN osm o ON o.nat_ref = n.villcode
),
metrics AS (
    SELECT
        county,
        town,
        village,
        osm_id,
        ST_Area(ng)                                              AS area_nlsc,
        ST_Area(og)                                             AS area_osm,
        ST_Area(og) - ST_Area(ng)                               AS area_diff,
        CASE
            WHEN ST_Area(ST_Union(ng, og)) = 0 THEN NULL
            ELSE ST_Area(ST_Intersection(ng, og))::float
               / ST_Area(ST_Union(ng, og))::float
        END                                                      AS iou,
        ST_HausdorffDistance(ng, og)                            AS hausdorff
    FROM pairs
)
SELECT
    county,
    town,
    village,
    osm_id,
    ROUND(area_nlsc::numeric, 2)  AS area_nlsc_m2,
    ROUND(area_osm::numeric,  2)  AS area_osm_m2,
    ROUND(area_diff::numeric, 2)  AS area_diff_m2,
    ROUND(iou::numeric,       6)  AS iou,
    ROUND(hausdorff::numeric, 2)  AS hausdorff_m
FROM metrics
ORDER BY county, town, village;
"""

FIELDNAMES = [
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

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(FIELDNAMES)
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {OUTPUT_FILE}")

    # Quick summary stats
    matched = len(rows)
    if matched:
        iou_vals = [float(r[7]) for r in rows if r[7] is not None]
        if iou_vals:
            print(f"IoU   — min: {min(iou_vals):.4f}  avg: {sum(iou_vals)/len(iou_vals):.4f}  max: {max(iou_vals):.4f}")


if __name__ == "__main__":
    main()
