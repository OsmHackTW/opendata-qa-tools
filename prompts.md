Set up a Docker Compose project in `boundaries`, with `postgis/postgis:18-3.6` as database, and a `python:3.14-slim`
as the script runner.

Write down Python scripts in `boundaries` that will eventually executed in `runner` of Docker Compose, to import the boundaries in Taiwan from NLSC (ground truth) and OpenStreetMap overpass turbo.
Put the resulting data into two table named `boundaries_nlsc` and `boundaries_osm`.
Both table should feature a `geom` and `properties` column; `geom` should be a WGS84 `geometry` and `properties` should be a `jsonb`.

- use `uv` to maintain dependencies and CP the `uv:0.10` binary in `worker` Dockerfile to sync the dependencies.
- For NLSC, download https://maps.nlsc.gov.tw/download/%E6%9D%91(%E9%87%8C)%E7%95%8C(TWD97%E7%B6%93%E7%B7%AF%E5%BA%A6).zip and convert the coordinate from TWD97 to WGS84.
If there is multiple SHP file in the ZIP, only import the one with largest file size.
- For OpenStreetMap, download all nested member from relation `https://www.openstreetmap.org/relation/449220` with Overpass query.
Import only relationship with `boundary=administrative` and `admin_level=9` into the table as multipolygons.


Write down a `compare.py` to compare the resulting `boundaries_nlsc` and `boundaries_osm`.
- Use the `VILLCODE` from NLSC and `nat_ref` from OSM to find references.
- Output a CSV file with the following column:
 * country, town and village name (from `COUNTYNAME`, `TOWNNAME`, `VILLNAME` in NLSC)
 * OSM relation ID
 * IoU, Area Diff and Hausdorff distance, area in both NLSC and OSM
- If possible, just use PostGIS to calculate them

It should be ran by `runner` after both imports.

the import_nslc failes with ` certificate verify failed: Missing Subject Key Identifier`

* add a timed-delay retry on overpass queries
* Fix AttributeError: 'Polygon' object has no attribute '__contains__' in import_osm.py

add .gitignore to ignore `.venv` in boundaries and commit the whole changes with `feat: add Claude Code vibe-coded village boundaries verifier`

Can you try to add more checks
* Still output the village row if village presents in NLSC but not OSM
* Output orphan OSM relationships