# purr_geographix

<div style="display: flex; height: 85px">
    <img src="./docs/purrio.png" alt="drawing" height="85px" style="margin-right: 20px;"/>
    <img src="./docs/geographix.png" alt="drawing"/>
</div>

Use **purr_geographix** to locate and query any GeoGraphix project* with zero setup via a
simple Python API. It's the missing middleware for taming an unruly geoscience data
environment.

Check out the (Swagger)
[API](https://rbhughes.github.io/purr_geographix/)

---

* #### Dynamic discovery of projects (even lost, unshared projects)
* #### Simple Python API
* #### No license checkouts
* #### Well-centric exports to JSON:

```
- completion
- core
- dst
- formation
- ip
- perforation
- production
- raster_log
- survey
- vector_log
- well
- zone
```

* _Each [GeoGraphix](https://www.gverse.com/) "project" is a semi-structured collection
  of E&P assets that interoperate with its own
  [SQLAnywhere](https://www.sap.com/products/technology-platform/sql-anywhere.html)
  database. From an IT perspective, GeoGraphix is a distributed collection of
  user-managed databases "on the network" containing millions of assets._

## Quickstart

---

#### installation:

`pip install purr_geographix`

#### launch:

`uvicorn purr_geographix.main:app --workers 4`

**purr_geographix** uses [FastAPI](https://fastapi.tiangolo.com "FastAPI").
You can test-drive your local API at: `http://localhost:8000/docs`

## Usage

---

#### 1. Do a POST `/purr/ggx/repos/recon` with a network path containing GeoGraphix

projects (or just a path to a single project) and a GeoGraphix server hostname.

_We use the term **repo** and **project** interchangeably_

![recon](./docs/recon.png)

or

```
curl -X 'POST' \
'http://localhost:8000/purr/ggx/repos/recon?recon_root=%5C%5Cscarab%5Cggx_projects&ggx_host=scarab' \
-H 'accept: application/json' \
-d ''
```

_(Replace single quotes with double-quote for Windows)_

This might take a few minutes, so you get a `202 Reponse` containing a task id and
task_status:

```
{
  "id": "d0ce171c-3f0f-4b37-953b-fe9df2f14bd1",
  "recon_root": "\\\\scarab\\ggx_projects\\",
  "ggx_host": "scarab",
  "task_status": "pending"
}
```

Metadata for each [repo](./docs/colorado_north.json) is stored in a local (sqlite)
database.

#### 2. Use the task id to check status with a GET to `/purr/ggx/repos/recon/{task_id}`

```
{
  "id": "d0ce171c-3f0f-4b37-953b-fe9df2f14bd1",
  "recon_root": "\\\\scarab\\ggx_projects\\",
  "ggx_host": "scarab",
  "task_status": "completed"
}
```

Possible status values are: `pending`, `in_progress`, `completed`, or `failed`.

#### 3. Use the `repo_id` to query asset data in a repo. Add a UWI filter to search for specific well identifiers.

![asset_0](./docs/asset_co_0.png)

This can also be time-consuming, so it returns a `202 Response` with a task id and the
pending export file.

```
{
  "id": "f1de6c36-5693-4cbb-a4a1-9327af501ca1",
  "task_status": "pending",
  "task_message": "export file (pending): col_7159c5_1721489912_completion.json"
}
```

#### 4. Doing a GET to /purr/ggx/asset/{repo_id}/{asset} returns tast_status and a

task_message containing exported file info.

```
{
"id": "f1de6c36-5693-4cbb-a4a1-9327af501ca1",
"task_status": "completed",
"task_message": "Exported 72 docs to: C:\\temp\\col_7159c5_1721489912_completion.json"
}
```

All asset data is exported as a "flattened" JSON representation of the original
relational model. Here's a [survey](./docs/survey.json) example.

---

## Future

Let me know whatever you might want to see in a future release. Some ideas are:

* Better query logic: match terms with `AND` instead of `SIMILAR TO` (`OR`)
* Structured ASCII (Petra PPF or GeoGraphix ASCII3) exports instead of JSON
* Full Text Search
* Datum-shift and standardize on EPSG:4326 for polygon hull points
* Auto-sync with your PPDM or OSDU store?
* Standardize a multi-project interface with Spotfire

## License

```
MIT License

Copyright (c) 2024 Bryan Hughes

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```