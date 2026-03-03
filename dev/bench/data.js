window.BENCHMARK_DATA = {
  "lastUpdate": 1772543357342,
  "repoUrl": "https://github.com/fintermobilityas/surge",
  "entries": {
    "Surge (small)": [
      {
        "commit": {
          "author": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "committer": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "distinct": true,
          "id": "fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5",
          "message": "Fix benchmark workflow: add write permissions for gh-pages push",
          "timestamp": "2026-03-03T14:07:26+01:00",
          "tree_id": "0cee25658616c3b055acfee73ddc2678c7728042",
          "url": "https://github.com/fintermobilityas/surge/commit/fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5"
        },
        "date": 1772543336663,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 17.292160000000003,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 16.269151,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 28.465595,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 7.756888,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 9.132423000000001,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 7.280523,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 5.593941,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 1.505065,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 620.658394,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 47.259685000000005,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 694.636321,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 46.953677,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 16.114022,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 631.522601,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 47.370685,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 236.979772,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.09178099999999999,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 21.746835,
            "unit": "ms"
          }
        ]
      }
    ],
    "Surge (large)": [
      {
        "commit": {
          "author": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "committer": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "distinct": true,
          "id": "fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5",
          "message": "Fix benchmark workflow: add write permissions for gh-pages push",
          "timestamp": "2026-03-03T14:07:26+01:00",
          "tree_id": "0cee25658616c3b055acfee73ddc2678c7728042",
          "url": "https://github.com/fintermobilityas/surge/commit/fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5"
        },
        "date": 1772543353416,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 77.037447,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 107.022141,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 139.955116,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 78.171649,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 87.098444,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 44.845612,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 49.805758999999995,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 21.639722,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 8786.962245,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 461.806758,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 4769.770807000001,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 274.86812199999997,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 108.657233,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 4684.3091540000005,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 276.52319,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 1367.4148539999999,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.08355599999999999,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 161.64731700000002,
            "unit": "ms"
          }
        ]
      }
    ],
    "Surge (medium)": [
      {
        "commit": {
          "author": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "committer": {
            "email": "peter.sunde@gmail.com",
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters"
          },
          "distinct": true,
          "id": "fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5",
          "message": "Fix benchmark workflow: add write permissions for gh-pages push",
          "timestamp": "2026-03-03T14:07:26+01:00",
          "tree_id": "0cee25658616c3b055acfee73ddc2678c7728042",
          "url": "https://github.com/fintermobilityas/surge/commit/fe9fc0cb66567dd35bd1e7f84d03121b720f0cc5"
        },
        "date": 1772543357069,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 45.833845,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 62.86008999999999,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 93.230518,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 39.242958,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 43.80215,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 25.050552,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 27.231675,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 12.142297,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 4308.793753,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 243.333508,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 4309.716533,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 245.58282,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 65.18926,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 4312.251296,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 247.668486,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 1229.726646,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.084838,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 90.527795,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}