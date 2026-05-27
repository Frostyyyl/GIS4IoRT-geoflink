import json
import math
import time
from functools import partial

import rclpy
from rclpy.node import Node
from sensor_msgs.msg import NavSatFix
from kafka import KafkaProducer


class MultiRobotKafkaBridge(Node):
    def __init__(self):
        super().__init__('multi_robot_kafka_bridge')

        # --- PARAMETERS ---
        self.declare_parameter('robot_list', ['robot_0', 'robot_1'])
        self.declare_parameter('kafka_bootstrap_servers', 'localhost:9092')
        self.declare_parameter('kafka_topic', 'multi_gps_fix')

        self.robot_list = self.get_parameter('robot_list').get_parameter_value().string_array_value
        self.kafka_servers = self.get_parameter('kafka_bootstrap_servers').get_parameter_value().string_value
        self.target_kafka_topic = self.get_parameter('kafka_topic').get_parameter_value().string_value

        self.get_logger().info(f'Connecting to Kafka: {self.kafka_servers}')
        self.get_logger().info(f'Target Kafka Topic: {self.target_kafka_topic}')

        # Initialize Kafka Producer with JSON serialization
        self.producer = KafkaProducer(
            bootstrap_servers=[self.kafka_servers],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            acks=1,
            retries=3
        )

        self.subscriptions_list = []

        # Dynamically create subscriptions for each robot in the list
        for robot_name in self.robot_list:
            ros_topic = f'/{robot_name}/gps/fix'
            
            # Bind robot_name to the callback to identify source in the handler
            callback_with_name = partial(self.gps_callback, robot_name=robot_name)
            
            sub = self.create_subscription(
                NavSatFix,
                ros_topic,
                callback_with_name,
                10
            )
            self.subscriptions_list.append(sub)
            self.get_logger().info(f'Subscribed to: {ros_topic} for robot: {robot_name}')

    def gps_callback(self, msg, robot_name):
        # Validate GPS data integrity
        if math.isnan(msg.latitude) or math.isnan(msg.longitude):
            self.get_logger().warn(f"Skipping NaN GPS from {robot_name}")
            return

        timestamp_ms = int(time.time() * 1000)

        payload = {
            'id': robot_name,
            'ts': timestamp_ms,
            'lat': msg.latitude,
            'lon': msg.longitude
        }

        try:
            # Send to Kafka using robot_name as key to ensure partition ordering
            self.producer.send(
                self.target_kafka_topic,
                value=payload,
                key=robot_name
            )
        except Exception as e:
            self.get_logger().error(f'Failed to send to Kafka: {e}')


def main(args=None):
    rclpy.init(args=args)
    node = MultiRobotKafkaBridge()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        node.get_logger().info("Stopped by the user (Ctrl+C)")
    except rclpy.executors.ExternalShutdownException:
        node.get_logger().info("ROS2 context closed (SIGTERM)")
    finally:
        # Ensure any buffered messages are sent before exit
        node.producer.flush()
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()