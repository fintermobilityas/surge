window.BENCHMARK_DATA = {
  "lastUpdate": 1772863807206,
  "repoUrl": "https://github.com/fintermobilityas/surge",
  "entries": {
    "Surge Micro (small)": [
      {
        "commit": {
          "author": {
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters",
            "email": "peter.sunde@gmail.com"
          },
          "committer": {
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters",
            "email": "peter.sunde@gmail.com"
          },
          "id": "03a31c12c8ff93d7f2e9716e9ab79448f79cb384",
          "message": "test(core): stabilize archive delta demoapp regression",
          "timestamp": "2026-03-06T20:35:23Z",
          "url": "https://github.com/fintermobilityas/surge/commit/03a31c12c8ff93d7f2e9716e9ab79448f79cb384"
        },
        "date": 1772863625380,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=3)",
            "value": 108.326425,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 79.125371,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 5.072948,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 5.815237,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 79.66872699999999,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 12.615295,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 8769.443961,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 433.723897,
            "unit": "ms"
          }
        ]
      }
    ],
    "Surge Micro (medium)": [
      {
        "commit": {
          "author": {
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters",
            "email": "peter.sunde@gmail.com"
          },
          "committer": {
            "name": "Peter Rekdal Khan-Sunde",
            "username": "peters",
            "email": "peter.sunde@gmail.com"
          },
          "id": "03a31c12c8ff93d7f2e9716e9ab79448f79cb384",
          "message": "test(core): stabilize archive delta demoapp regression",
          "timestamp": "2026-03-06T20:35:23Z",
          "url": "https://github.com/fintermobilityas/surge/commit/03a31c12c8ff93d7f2e9716e9ab79448f79cb384"
        },
        "date": 1772863806927,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Archive create (zstd=3)",
            "value": 488.392157,
            "unit": "ms"
          },
          {
            "name": "Archive extract",
            "value": 297.768028,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (in-memory)",
            "value": 25.253579000000002,
            "unit": "ms"
          },
          {
            "name": "SHA-256 (file)",
            "value": 27.529501,
            "unit": "ms"
          },
          {
            "name": "Zstd compress (level=3)",
            "value": 388.61286,
            "unit": "ms"
          },
          {
            "name": "Zstd decompress",
            "value": 49.712277,
            "unit": "ms"
          },
          {
            "name": "chunked bsdiff",
            "value": 77809.98757499999,
            "unit": "ms"
          },
          {
            "name": "chunked bspatch",
            "value": 2070.241666,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}