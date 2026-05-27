FROM ros:jazzy-ros-base
RUN apt update && apt install -y \
    python3-pip \
    python3-venv \
    ros-jazzy-rmw-cyclonedds-cpp \
    ros-jazzy-sensor-msgs

RUN python3 -m venv /venv

ENV PATH="/venv/bin:$PATH"
RUN pip install --upgrade pip && \
    pip install kafka-python pyyaml numpy
