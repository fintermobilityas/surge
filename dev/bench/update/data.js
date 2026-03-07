window.BENCHMARK_DATA = {
  "lastUpdate": 1772867630490,
  "repoUrl": "https://github.com/fintermobilityas/surge",
  "entries": {
    "Surge Update (localized-chain)": [
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
        "date": 1772866396514,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Publish 101 releases",
            "value": 1733016.662262,
            "unit": "ms"
          },
          {
            "name": "Update check (100 deltas)",
            "value": 2.941337,
            "unit": "ms"
          },
          {
            "name": "Update apply (100 deltas)",
            "value": 367911.167285,
            "unit": "ms"
          }
        ]
      }
    ],
    "Surge Update (broad-chain)": [
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
        "date": 1772867630080,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "Publish 11 releases",
            "value": 1083377.258342,
            "unit": "ms"
          },
          {
            "name": "Update check (10 deltas)",
            "value": 0.466905,
            "unit": "ms"
          },
          {
            "name": "Update apply (10 deltas)",
            "value": 48777.231873,
            "unit": "ms"
          }
        ]
      }
    ]
  }
}