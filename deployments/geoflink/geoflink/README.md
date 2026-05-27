# Flink for GIS4IoRT Chist-Era project

## Project Structure & Attribution

This project is an extension of the **GeoFlink** library. The codebase is organized into two main packages to clearly distinguish original work from the base library:

* `src/main/java/GeoFlink`: Contains the original source code of the **SpatialFlink/GeoFlink** library (Copyright AIST). This code provides the core spatial stream processing primitives.
* `src/main/java/GIS4IoRT`: Contains **custom implementation and new operators** developed specifically for this thesis. This package includes the logic for geofencing, collision detection and sensor proximity query types.

### License
The original GeoFlink code is licensed under **Apache License 2.0**. The modifications and new modules in `GIS4IoRT` are compatible with this license. The original GeoFlink license is provided in the file LICENSE-SpatialFlink.