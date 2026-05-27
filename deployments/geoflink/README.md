# GeoFlink for GIS4IoRT Chist-Era Project

> **Part of the thesis:** Architecture for Integrating Robotic Devices: Design, Implementation, Experimental Evaluation

This module implements the **real-time stream processing architecture** based on **Apache Flink**. It integrates and extends the capabilities of the **GeoFlink** library to handle high-velocity telemetry data from autonomous robots. The system performs spatial operations such as collision detection, dynamic geofencing, and sensor data fusion in real-time.

---

## Usage

Follow the steps below to deploy the infrastructure, run visualizations, and execute processing benchmarks.

### 1. Infrastructure Deployment

First, deploy the core messaging and processing infrastructure (Apache Kafka, Apache Flink Cluster).

**Directory:** `GIS4IoRT-processing-layer/deployments/geoflink/docker`

```bash
docker-compose -f docker-compose.infra.yml up -d --build

```

### 2. Bridge & Visualization Setup

Deploy the ROS2 bridge and the visualization dashboard.

**Directory:** `GIS4IoRT-processing-layer/deployments/geoflink/docker`

```bash
docker-compose -f docker-compose.viz.yml up -d --build

```

> **OS Compatibility Warning**
> For the visualization components to function correctly (specifically GUI forwarding), this stack **must be executed on a native Linux environment or WSL 2 (Windows Subsystem for Linux)**.

---

### 3. Running Automated Benchmarks

To execute performance tests similar to those presented in the thesis experimental evaluation:

**Directory:** `GIS4IoRT-processing-layer/deployments/geoflink/benchmarks`

**Configuration Steps:**

1. **Define Inputs:** Configure input data (robots and land parcels) in `setup_geoflink.py`.
2. **Define Test Cases:** Specify Flink job parameters and scenarios in `flink_manager.py`.
3. **Set Runtime Params:** Adjust iteration parameters in `run_tests.py`.

**Execution:**

1. Initialize the environment and Kafka topics:
```bash
python setup_geoflink.py

```


2. Run the benchmark procedure:
```bash
python run_tests.py

```



---

### 4. General Processing & Manual Execution

If you wish to run the system manually or interactively outside of the benchmark procedure:

#### A. System Configuration (API)

Before processing begins, you must register the monitoring query, robots, and zones.

This is exposed via a standard **REST API**. You can send configuration payloads using any HTTP client (e.g., cURL, Postman, custom scripts) or use the interactive documentation provided below.

* **API Base URL:** `http://localhost:8000`
* **Interactive UI (Swagger):** [http://localhost:8000/docs](http://localhost:8000/docs)
* **Action:** Send `POST` requests to relevant endpoints to register monitored objects.

#### B. Sensor Data Generation (Optional)

To simulate environmental sensor readings synchronized with robot movement:

**Directory:** `GIS4IoRT-processing-layer/deployments/geoflink/benchmarks`

1. Open `kafka_sensor_producer.py` and set the target **robot name** variable to match your configuration.
2. Run the producer:
```bash
python kafka_sensor_producer.py --robot_topic "INPUT_ROBOT_TOPIC" --sensor_topic "OUTPUT_SENSOR_TOPIC"

```



#### C. Replaying ROS2 Data (Rosbag)

To inject telemetry data from a recorded session into the system:

> **Note:** The system expects GPS telemetry data to be published on topics using the **`sensor_msgs/msg/NavSatFix`** message type.

**Directory:** `GIS4IoRT-processing-layer/deployments/geoflink/docker`

1. **Prerequisite:** Ensure your `.mcap` or `.db3` rosbag file is placed in `GIS4IoRT-processing-layer/deployments/geoflink/docker/data` **before** building the docker stack in Step 2.
2. **Execution:** Run the following command to start playback inside the bridge container:
```bash
docker exec -it ros_bridge bash -c "source /opt/ros/jazzy/setup.bash && ros2 bag play /app/data/YOUR_ROSBAG_FILE --rate 10 --topics YOUR_TOPIC_NAME"

```

## Credits

This project was built using open-source technologies. We would like to explicitly acknowledge the following library:

**GeoFlink (SpatialFlink)**: The spatial stream processing engine used in the GeoFlink implementation is based on the [SpatialFlink](https://github.com/aistairc/SpatialFlink) library developed by **AIST Artificial Intelligence Research Center**.
* *Modifications:* The library source code is included in the `GIS4IoRT-processing-layer\deployments\geoflink\geoflink\src\main\java\GeoFlink` package to support custom extensions.
* *License:* Apache License 2.0. See `GIS4IoRT-processing-layer\deployments\geoflink\geoflink\LICENSE-SpatialFlink` for detailed attribution and license information.


