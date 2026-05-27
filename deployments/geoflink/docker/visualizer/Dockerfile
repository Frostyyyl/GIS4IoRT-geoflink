FROM ros:jazzy-ros-base
RUN apt update && apt install -y \
    python3-pip \
    python3-venv \
    ros-jazzy-visualization-msgs \
    ros-jazzy-rmw-cyclonedds-cpp \
    ros-jazzy-geometry-msgs \
    ros-jazzy-sensor-msgs \
    ros-jazzy-builtin-interfaces

RUN python3 -m venv /venv

ENV PATH="/venv/bin:$PATH"
RUN pip install --upgrade pip && \
    pip install pandas shapely pyproj pyyaml

