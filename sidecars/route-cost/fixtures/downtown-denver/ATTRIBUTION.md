# Downtown Denver test fixture

These graph files are copied unmodified from the RouteE-Compass
repository's `downtown_denver_example` resources:

- https://github.com/NREL/routee-compass
- License: BSD-3-Clause, Alliance for Sustainable Energy, LLC (NREL).
  See LICENSE-routee-compass.md in this directory.

The road network itself is derived from OpenStreetMap data:
(c) OpenStreetMap contributors, available under the Open Database
License (ODbL), https://www.openstreetmap.org/copyright

The fixture exists solely as a self-contained smoke-test dataset for
the `route-cost` sidecar. It covers roughly downtown Denver, Colorado;
coordinates outside that box will fail map matching, which the tests
rely on to exercise the error path.

`travel-time.toml` is ours (written for this sidecar), not NREL's.
