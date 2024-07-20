# purr_geographix

<div style="display: flex; align-items: center; height: 100px">
    <img src="./docs/purrio.png" alt="drawing" width="100" style="margin-right: 20px;"/>
    <img src="./docs/geographix.png" alt="drawing" width="300"/>
</div>

A mature GeoGraphix environment may contain dozens of projects* in various
states of neglect. Some may exist on Windows shares not managed by a project server,
thus making them invisible to the Discovery interface.

Use purr_geographix to easily locate and query any GeoGraphix project via a simple
Python API. It's the missing middleware for taming an unruly geoscience data environment.

---

* ### Dynamic discovery of projects (even "misplaced" projects)
* ### Simple Python API
* ### No license checkouts
* ### Well-centric exports to JSON:

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

---

* Each [GeoGraphix](https://www.gverse.com/) "project" is a semi-structured collection
  of E&P assets that interoperate with
  a [SQLAnywhere](https://www.sap.com/products/technology-platform/sql-anywhere.html)
  database. A typical mid-continent US project may contain a half million well records,
  hundreds of [ESRI](https://www.esri.com/en-us/home)-powered maps and thousands of
  files.

## Installation

`pip install purr_geographix`

## Usage

<img src="./docs/fastapi.png" alt="drawing" width="100"/>

purr_geographix is based on [FastAPI](https://fastapi.tiangolo.com "FastAPI").
Once installed, you can use the auto-generated Swagger API pages to test drive
the available routes. The current (demo) documentation:
[purr_geographix routes](https://rbhughes.github.io/purr_geographix/)



---

#### A note about using `curl` on Windows

The API docs show curl examples using Linux syntax with a lot of "\" to represent line
continuations. Powershell and WSL might be able to handle them as-is. Replace single
quotes with double-quotes if using cmd.exe.

```
curl -X 'GET' \
  'http://localhost:8000/purr/ggx/repos/' \
  -H 'accept: application/json'
```

...will probably work if you adjust to...

`curl -X "GET" "http://localhost:8000/purr/ggx/repos/" -H "accept: application/json"`

...or even...

`curl -X GET http://localhost:8000/purr/ggx/repos/ -H accept: application/json`

Another example using POST

curl -X 'POST' \
'http://localhost:8000/purr/ggx/repos/recon?recon_root=%5C%5Cscarab%5Cggx_projects&ggx_host=scarab' \
-H 'accept: application/json' \
-d ''

curl -X 'POST' \
'http://localhost:8000/purr/ggx/asset/COL_7159C5/well?uwi_query=05045150%2A' \
-H 'accept: application/json' \
-d ''

http://localhost:8000/purr/ggx/asset/COL_7159C5/well?uwi_query=05045150%2A

## Future

Let me know whatever you might want to see in a future release. Some ideas are:

* Structured ASCII (Petra PPF or GeoGraphix ASCII3) exports instead of JSON
* Full Text Search
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