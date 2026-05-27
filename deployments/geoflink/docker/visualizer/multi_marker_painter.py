import os
import json
import binascii
from math import sqrt, cos, sin, pi
from functools import partial

import pandas as pd
from shapely import wkb, geometry, affinity
from shapely.ops import unary_union
from pyproj import Transformer

import rclpy
from rclpy.node import Node
from visualization_msgs.msg import Marker
from geometry_msgs.msg import Point
from builtin_interfaces.msg import Duration
from sensor_msgs.msg import NavSatFix


class CsvToRvizMarkers(Node):
    def __init__(self):
        super().__init__('csv_to_rviz_markers')

        self.publisher = self.create_publisher(Marker, 'visualization_marker', 10)

        self.csv_path = os.path.expanduser('/app/data/parcelles.csv')
        self.sensor_config_path = os.path.expanduser('/app/data/sensor_config_updated.json')
        
        # CRS Transformer: WGS84 (GPS) -> EPSG:2154 (Lambert 93)
        self.transformer = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True)
        self.global_centroid = None
        self.cached_df = None

        # --- PARAMETERS ---
        self.declare_parameter('robot_list', ['leader', 'follower'])
        self.robot_list = self.get_parameter('robot_list').get_parameter_value().string_array_value

        self.declare_parameter('centroid_shape_name', '1 MONT')
        self.centroid_shape_name = self.get_parameter('centroid_shape_name').get_parameter_value().string_value

        self.declare_parameter('target_parcel_id', 40)
        self.target_parcel_id = self.get_parameter('target_parcel_id').get_parameter_value().integer_value

        self.robot_radius = 3.0
        self.VISUAL_BUFFER = 0.5

        self.robot_paths = {}
        self.robot_colors = {}
        self.subscriptions_list = []

        # --- PRE-CALCULATION ---
        # Generate circle points once to reduce CPU load during callbacks
        self.circle_template_points = []
        num_segments = 64
        calc_radius = self.robot_radius + self.VISUAL_BUFFER
        for i in range(num_segments + 1):
            angle = (i / num_segments) * 2 * pi
            px = calc_radius * cos(angle)
            py = calc_radius * sin(angle)
            self.circle_template_points.append(Point(x=px, y=py, z=0.0))

        colors = [
            (0.0, 0.0, 1.0), (0.0, 1.0, 0.0), (1.0, 0.0, 0.0),
            (1.0, 1.0, 0.0), (0.0, 1.0, 1.0), (1.0, 0.0, 1.0)
        ]

        self.get_logger().info(f"Initialization for robots: {self.robot_list}")

        # Initialize markers and subscribers per robot
        for i, robot_name in enumerate(self.robot_list):
            path_marker = Marker()
            path_marker.header.frame_id = 'map'
            path_marker.ns = 'robot_paths'
            path_marker.type = Marker.LINE_STRIP
            path_marker.action = Marker.ADD
            path_marker.id = 10000 + i
            path_marker.scale.x = 0.7

            r, g, b = colors[i % len(colors)]
            path_marker.color.r = r
            path_marker.color.g = g
            path_marker.color.b = b
            path_marker.color.a = 1.0
            path_marker.points = []
            path_marker.pose.orientation.w = 1.0

            self.robot_paths[robot_name] = path_marker
            self.robot_colors[robot_name] = (r, g, b)

            gps_topic = f'/{robot_name}/gps/fix'
            callback = partial(self.gps_callback, robot_name=robot_name, robot_index=i)
            sub = self.create_subscription(NavSatFix, gps_topic, callback, 10)
            self.subscriptions_list.append(sub)

        self.timer = self.create_timer(2.0, self.publish_map_markers)

    def publish_map_markers(self):
        if not os.path.exists(self.csv_path):
            return

        # Cache CSV to avoid I/O on every timer tick
        if self.cached_df is None:
            try:
                self.cached_df = pd.read_csv(self.csv_path)
            except Exception:
                return
        df = self.cached_df

        # Determine Global Centroid (0,0 point)
        if self.global_centroid is None:
            metric_geometries, centroid_geometries = [], []
            for i, row in df.iterrows():
                try:
                    geom = wkb.loads(binascii.unhexlify(row['geom']))
                    polygons_list = [geom] if geom.geom_type == 'Polygon' else list(geom.geoms)
                    for poly in polygons_list:
                        # Project coordinates to metric system
                        exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                        proj_poly = geometry.Polygon(exterior)
                        metric_geometries.append(proj_poly)
                        if row['name'] == self.centroid_shape_name:
                            centroid_geometries.append(proj_poly)
                except Exception:
                    pass

            if not metric_geometries:
                return
            
            # Prefer named shape for centroid, otherwise use union of all shapes
            combined = unary_union(centroid_geometries) if centroid_geometries else unary_union(metric_geometries)
            self.global_centroid = combined.centroid
            self.get_logger().info(f"Centroid: {self.global_centroid.x}, {self.global_centroid.y}")

        # Draw Parcels relative to Centroid
        for i, row in df.iterrows():
            if self.target_parcel_id != -1 and int(row['id']) != self.target_parcel_id:
                continue
            try:
                geom = wkb.loads(binascii.unhexlify(row['geom']))
                polygons_list = [geom] if geom.geom_type == 'Polygon' else list(geom.geoms)

                metric_polys = []
                for poly in polygons_list:
                    exterior = [self.transformer.transform(y, x) for x, y in poly.exterior.coords]
                    metric_polys.append(geometry.Polygon(exterior))

                metric_geom = geometry.MultiPolygon(metric_polys) if len(metric_polys) > 1 else metric_polys[0]
                
                # Shift geometry to local frame (map frame)
                shifted = affinity.translate(metric_geom, xoff=-self.global_centroid.x, yoff=-self.global_centroid.y)

                marker = Marker()
                marker.header.frame_id = 'map'
                marker.ns = 'parcels'
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.id = int(row['id'])
                marker.scale.x = 0.5
                marker.color.r = 1.0
                marker.color.a = 1.0
                marker.pose.orientation.w = 1.0

                polys_to_draw = shifted.geoms if shifted.geom_type == 'MultiPolygon' else [shifted]
                for p in polys_to_draw:
                    for x, y in p.exterior.coords:
                        marker.points.append(Point(x=x, y=y, z=0.0))
                self.publisher.publish(marker)
            except Exception:
                pass

        self.publish_sensor_markers()

    def publish_sensor_markers(self):
        if not os.path.exists(self.sensor_config_path) or self.global_centroid is None:
            return
        try:
            with open(self.sensor_config_path, 'r') as f:
                sensors = json.load(f)

            for s in sensors:
                sid = s.get('sensor_id')
                lat, lon = s.get('lat'), s.get('lon')
                radius = s.get('radius', 30.0) + self.VISUAL_BUFFER

                mx, my = self.transformer.transform(lat, lon)
                cx, cy = mx - self.global_centroid.x, my - self.global_centroid.y

                marker = Marker()
                marker.header.frame_id = 'map'
                marker.ns = 'sensors'
                marker.type = Marker.LINE_STRIP
                marker.action = Marker.ADD
                marker.id = int(sid)
                marker.scale.x = 0.5
                marker.color.r = 1.0
                marker.color.g = 0.65
                marker.color.a = 1.0
                marker.pose.orientation.w = 1.0

                # Generate circle for sensor zone
                num = 64
                for i in range(num + 1):
                    angle = (i / num) * 2 * pi
                    marker.points.append(Point(x=cx + radius * cos(angle), y=cy + radius * sin(angle), z=0.0))
                self.publisher.publish(marker)
        except Exception:
            pass

    def gps_callback(self, msg: NavSatFix, robot_name: str, robot_index: int):
        if self.global_centroid is None:
            return

        # Convert GPS to Local Map Coordinates
        mx, my = self.transformer.transform(msg.latitude, msg.longitude)
        dx = mx - self.global_centroid.x
        dy = my - self.global_centroid.y

        current_time = self.get_clock().now().to_msg()

        # Robot Footprint (Circle)
        circle_marker = Marker()
        circle_marker.header.frame_id = 'map'
        circle_marker.header.stamp = current_time
        circle_marker.ns = 'robot_circles'
        circle_marker.id = 20000 + robot_index
        circle_marker.type = Marker.LINE_STRIP
        circle_marker.action = Marker.ADD
        circle_marker.scale.x = 0.6

        r, g, b = self.robot_colors[robot_name]
        circle_marker.color.r = r
        circle_marker.color.g = g
        circle_marker.color.b = b
        circle_marker.color.a = 1.0

        circle_marker.lifetime = Duration(sec=2, nanosec=0)

        circle_marker.pose.position.x = dx
        circle_marker.pose.position.y = dy
        circle_marker.pose.orientation.w = 1.0

        circle_marker.points = self.circle_template_points
        self.publisher.publish(circle_marker)

        # Robot Path
        path_marker = self.robot_paths[robot_name]
        new_point = Point(x=dx, y=dy, z=0.0)

        should_update = False
        if not path_marker.points:
            should_update = True
        else:
            last = path_marker.points[-1]
            dist = sqrt((dx - last.x)**2 + (dy - last.y)**2)
            
            # Reset path if distance jump is too large (to clear path when restarting)
            if dist > 5.0:
                path_marker.points = []
                should_update = True
            elif dist > 0.02:
                should_update = True

        if should_update:
            path_marker.points.append(new_point)
            path_marker.header.stamp = current_time
            self.publisher.publish(path_marker)


def main(args=None):
    rclpy.init(args=args)
    node = CsvToRvizMarkers()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == '__main__':
    main()