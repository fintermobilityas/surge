window.BENCHMARK_DATA = {
  "lastUpdate": 1772543696024,
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
      },
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
          "id": "052b303f18ef690a7f3408c549de2172c198072f",
          "message": "Bump next-version to 0.3.0, document release process in AGENTS.md",
          "timestamp": "2026-03-03T14:12:46+01:00",
          "tree_id": "3049b9a73c47e4253c7e3f51ce9261366f633fe1",
          "url": "https://github.com/fintermobilityas/surge/commit/052b303f18ef690a7f3408c549de2172c198072f"
        },
        "date": 1772543611519,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 17.089707,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 16.593964,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 29.053172,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 7.788215,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 9.410264,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 9.455959,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 9.665259,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 1.7379749999999998,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 666.2874380000001,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 48.640818,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 646.118692,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 48.628745,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 15.942351,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 661.757189,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 49.629998,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 247.27908100000002,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.120083,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 21.325339,
            "unit": "ms"
          }
        ]
      },
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
          "distinct": false,
          "id": "052b303f18ef690a7f3408c549de2172c198072f",
          "message": "Bump next-version to 0.3.0, document release process in AGENTS.md",
          "timestamp": "2026-03-03T14:12:46+01:00",
          "tree_id": "3049b9a73c47e4253c7e3f51ce9261366f633fe1",
          "url": "https://github.com/fintermobilityas/surge/commit/052b303f18ef690a7f3408c549de2172c198072f"
        },
        "date": 1772543647544,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 16.094971,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 15.673613999999999,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 28.189928,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 7.758946,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 8.718297999999999,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 6.747507,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 5.137248,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 1.373086,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 604.026242,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 47.385203000000004,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 598.9254000000001,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 46.203824999999995,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 15.558208,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 590.352848,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 47.418454,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 233.675748,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.077315,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 20.852399000000002,
            "unit": "ms"
          }
        ]
      },
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
          "id": "f9fb0f67b4d8a97c6170bf1d105e8bd379157fce",
          "message": "Bump next-version to 0.4.0 for next development cycle",
          "timestamp": "2026-03-03T14:13:32+01:00",
          "tree_id": "af46d3478673fc73cf922b9ed15be57e52548496",
          "url": "https://github.com/fintermobilityas/surge/commit/f9fb0f67b4d8a97c6170bf1d105e8bd379157fce"
        },
        "date": 1772543692076,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 17.248551,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 19.539227999999998,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 31.076871999999998,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 7.8225810000000005,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 9.292171,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 7.826588000000001,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 6.831536,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 2.043955,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 857.382858,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 49.564582,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 840.81897,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 49.763126,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 16.8632,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 811.697806,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 49.871377,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 252.04285900000002,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.109194,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 26.056115000000002,
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
      },
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
          "id": "f9fb0f67b4d8a97c6170bf1d105e8bd379157fce",
          "message": "Bump next-version to 0.4.0 for next development cycle",
          "timestamp": "2026-03-03T14:13:32+01:00",
          "tree_id": "af46d3478673fc73cf922b9ed15be57e52548496",
          "url": "https://github.com/fintermobilityas/surge/commit/f9fb0f67b4d8a97c6170bf1d105e8bd379157fce"
        },
        "date": 1772543687439,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 78.22999899999999,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 105.936644,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 141.429131,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 78.654197,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 86.925342,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 41.979618,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 56.014161,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 22.50761,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 8682.484253,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 461.958068,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 4638.675193,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 268.290161,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 107.113026,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 4597.504723,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 269.76493400000004,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 1356.107353,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.081342,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 158.71198,
            "unit": "ms"
          }
        ]
      },
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
          "distinct": false,
          "id": "052b303f18ef690a7f3408c549de2172c198072f",
          "message": "Bump next-version to 0.3.0, document release process in AGENTS.md",
          "timestamp": "2026-03-03T14:12:46+01:00",
          "tree_id": "3049b9a73c47e4253c7e3f51ce9261366f633fe1",
          "url": "https://github.com/fintermobilityas/surge/commit/052b303f18ef690a7f3408c549de2172c198072f"
        },
        "date": 1772543695554,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 84.845227,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 119.41094600000001,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 141.587873,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 88.888301,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 98.08917000000001,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 44.097156,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 53.442704,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 22.595789,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 9738.057398,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 468.993447,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 5173.3294430000005,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 276.744646,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 117.65499899999999,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 5156.537293,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 279.24172400000003,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 1379.3093350000001,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.062394,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 170.39423000000002,
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
      },
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
          "distinct": false,
          "id": "052b303f18ef690a7f3408c549de2172c198072f",
          "message": "Bump next-version to 0.3.0, document release process in AGENTS.md",
          "timestamp": "2026-03-03T14:12:46+01:00",
          "tree_id": "3049b9a73c47e4253c7e3f51ce9261366f633fe1",
          "url": "https://github.com/fintermobilityas/surge/commit/052b303f18ef690a7f3408c549de2172c198072f"
        },
        "date": 1772543649717,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=1)",
            "value": 44.215418,
            "unit": "ms"
          },
          {
            "name": "Archive create (zstd=3)",
            "value": 60.330968999999996,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 76.65195,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 39.640981,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 43.215365,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=1)",
            "value": 23.497688999999998,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 26.707357000000002,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 11.471926999999999,
            "unit": "ms"
          },
          {
            "name": "bsdiff",
            "value": 3996.037444,
            "unit": "ms"
          },
          {
            "name": "bspatch",
            "value": 234.715183,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 3964.066955,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 236.936018,
            "unit": "ms"
          },
          {
            "name": "Full package build",
            "value": 61.346581,
            "unit": "ms"
          },
          {
            "name": "Delta package build",
            "value": 3991.499664,
            "unit": "ms"
          },
          {
            "name": "Apply 1 delta",
            "value": 239.203213,
            "unit": "ms"
          },
          {
            "name": "Apply 5x deltas",
            "value": 1190.201708,
            "unit": "ms"
          },
          {
            "name": "Installer (web)",
            "value": 0.089737,
            "unit": "ms"
          },
          {
            "name": "Installer (offline)",
            "value": 87.254165,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}