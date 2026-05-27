# GIS4IoRT processing layer

This repository contains the implementation of the data integration layer developed as part of an engineering thesis:  
**"Architecture for Integrating Robotic Devices: Design, Implementation, Experimental Evaluation"**

> **University:** Poznan University of Technology  
> **Faculty:** Faculty of Computing and Telecommunications  
> **Field of Study:** Computer Science

---
## Project Overview

This engineering thesis addresses the challenge of efficient telemetry collection, normalization, and real-time analysis for heterogeneous robotic fleets within the context of **Agriculture 4.0** and the **Internet of Robotic Things (IoRT)**.

The primary goal was to bridge the interoperability gap between low-level robotic frameworks (**ROS 2**) and high-level analytical tools.

### Architectural Approach
Rather than proposing a single monolithic solution, this repository implements and compares **three distinct, end-to-end architectures**:
1.  **Apache Flink** (extended with GeoFlink)
2.  **ksqlDB**
3.  **NebulaStream**

Each architecture operates as an independent pipeline with its own dedicated ingestion mechanisms (bridges) and processing logic. This comparative design allows for a comprehensive assessment of how different stream processing paradigms handle high-frequency robotic workloads.

**Unified Access Layer**  
Regardless of the underlying architecture used, the processed results are exposed via a **FastAPI** service. This RESTful interface acts as a consistent consumption point for external applications.

---

## Implemented Spatial Use Cases

To validate the performance of the proposed architectures, three distinct types of spatial queries were implemented, representing common challenges in autonomous operations:

1.  **Geofencing (Safety-Critical Monitoring)**
    * **Logic:** A *Point-in-Polygon* operation that continuously monitors whether a robot remains within a pre-defined operational zone.
    * **Behavior:** An alert is triggered immediately if the robot's coordinates deviate from the designated safe area.

2.  **Sensor Proximity (Stream-to-Static Join)**
    * **Logic:** An event-driven query measuring the distance between a moving robot (stream) and static environmental sensors.
    * **Behavior:** An alert is generated if a robot enters the proximity of a sensor reporting critical values (e.g., high soil humidity).

3.  **Collision Detection (Stream-to-Stream Join)**
    * **Logic:** A dynamic proximity query that calculates Euclidean distances between multiple moving agents in real-time.
    * **Behavior:** Triggers an alert if the distance between any two robots falls below a safety threshold, requiring highly efficient windowing and state management strategies.

---
## Funding & Project Context

**This research is conducted as an integral part of the international GIS4IoRT project, supported by the National Science Centre (NCN), Poland, grant no. 2024/06/Y/ST6/00136, originally funded under the EU project Chist-Era call 2023, entitled Development of a Plug-and-Play Middleware for Integrating Robot Sensor Data with GIS Tools in a Cloud Environment.**


## Project Structure

The repository follows a clean separation of concerns, distinguishing between the unified access layer (API) and the specific stream processing implementations.

### `app/` (Unified Access Layer)
Contains the **FastAPI** application that serves as the single entry point for external systems.
* **Core Logic:** The main application factory and shared utilities.
* **Adapters (`app/adapters/{tech}/routers`):** Contains technology-specific endpoint definitions. Each architecture (GeoFlink, ksqlDB, NebulaStream) has its own router module here that connects the API to the underlying data streams.

### `deployments/` (Processing and Infrastructure)
Contains the core implementation of the stream processing architectures, custom operators, and deployment configurations.
* **Structure:** `deployments/{technology}/`
* **Contents per folder:**
    * **Docker Compose Stacks:** Complete environment definitions for running the specific architecture.
    * **Source Code:** Implementation of custom operators (e.g., Flink Jobs in Java).
    * **Benchmark Scripts:** Tools for logging each engine's results.

---

## Implemented Architectures 

The project implements and compares three distinct stream processing paradigms. Each module is self-contained with its own deployment logic.

### Apache Flink Architecture (GeoFlink)
* **Description:** A Java-based pipeline utilizing the **GeoFlink** extension for advanced spatial stream processing.
* **Focus:** High-throughput spatial joins and complex event processing.
* **Author:** Filip Baranowski
* **Documentation & Source:** [Go to GeoFlink Module](./deployments/geoflink/README.md)

### ksqlDB Architecture
* **Description:** A declarative, SQL-centric pipeline built directly on top of the Kafka ecosystem.
* **Focus:** Rapid development and ease of integration for standard filtering and aggregation tasks.
* **Author:** Antoni Sopata
* **Documentation & Source:** [Go to ksqlDB Module](./deployments/ksqldb/README.md)

### NebulaStream Architecture
* **Description:** An experimental pipeline using **NebulaStream**, a C++ based engine designed specifically for IoT environments.
* **Focus:** Low-latency processing and edge-cloud continuum capabilities.
* **Author:** Jakub Pilarski
* **Documentation & Source:** [Go to NebulaStream Module](./deployments/nebulastream/README.md)


## Credits

This project was built using open-source technologies. We would like to explicitly acknowledge the following libraries:

**GeoFlink (SpatialFlink)**: The spatial stream processing engine used in the GeoFlink implementation is based on the [SpatialFlink](https://github.com/aistairc/SpatialFlink) library developed by **AIST Artificial Intelligence Research Center**.
* *Modifications:* The library source code is included in the `GIS4IoRT-processing-layer\deployments\geoflink\geoflink\src\main\java\GeoFlink` package to support custom extensions.
* *License:* Apache License 2.0. See `GIS4IoRT-processing-layer\deployments\geoflink\geoflink\LICENSE-SpatialFlink` for detailed attribution and license information.

**ksqlDB**: The event streaming database used for the SQL-based implementation is based on [ksqlDB](https://ksqldb.io/) developed by **Confluent**.

**NebulaStream**: The distributed data management engine used for the NebulaStream-based implementation is derived from the [NebulaStream](https://nebula.stream/) project developed within the BIFOLD research initiative.