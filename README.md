# Quick start

## Prerequisites

You will need Docker Engine with Compose v2, Python 3.11

Setup python environment. In the terminal:

1. Navigate to the project root directory.

2. Create a python virtual environment: `python3.11 -m venv .venv`

3. Enter the virtual environment: `source .venv/bin/activate`

4. Download dependencies: `pip install -r requirements.txt`

## Running the system
Setup the Apache Kafka + Apache Flink + rosbag player ecosystem. [Look at this section](#quick-start).

Run Apache Kafka + Apache Flink + GIS API (with jar-uploader and UI's):

1. Navigate to the docker directory: `cd deployments/geoflink/docker`

2. Run the docker compose script: `docker compose -f docker-compose.infra.yml up -d --build`

Run the rosbag player:

1. Navigate to the docker directory: `cd deployments/geoflink/docker`

2. Run the docker compose script: `docker compose -f docker-compose.viz.yml up -d --build multi_kafka_bridge`

# Benchmarks

## Prerequisites
You will need Python 3.11.

Configure tests:
1. Find the `deployments/geoflink/benchmarks/config.py` file and define your test scenarios, including:
 
   - task templates,
   - number of robots and robot ids, 
   - zones,
   - collection time,
   - number of iterations.

Setup python environment. In the terminal:

1. Navigate to the benchmarks directory: `cd deployments/geoflink/benchmarks`

2. Create a python virtual environment: `python3.11 -m venv .venv`

3. Enter the virtual environment: `source .venv/bin/activate`

4. Download dependencies: `pip install -r requirements.txt`

## Running the tests
Start the Apache Kafka + Apache Flink + rosbag player ecosystem. [Look at this section](#quick-start).

Register zones, robots, configs and rules. In the terminal:

1. Navigate to the benchmarks directory: `cd deployments/geoflink/benchmarks`

2. Enter the virtual environment: `source .venv/bin/activate`

3. Run script: `python3 setup_geoflink.py`

4. Run script: `python3 run_tests.py`

### NOTE: Running benchmarks with the [ros2-robot-fleet-demo](https://github.com/LRMPUT/ros2-robot-fleet-demo) project
Instead of starting the ecosystem according to [this section](#quick-start), simply follow the instructions stated in the geoflink README.md file of the project found on the geoflink branch. Then proceed with the tests as usual.

## Analysis

The generated output data can be aggregated. 

1. In the `calculate_latency.py` file, set which metrics to compare.

2. Run the script: `calculate_latency.py <path/to/data>`
