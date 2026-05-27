import sys
import csv
import argparse
from rclpy.serialization import deserialize_message
from rosbag2_py import SequentialReader, StorageOptions, ConverterOptions, StorageFilter
from sensor_msgs.msg import NavSatFix

def main():
    parser = argparse.ArgumentParser(description="ROS 2 bag to CSV converter for GPS data.")
    parser.add_argument("bag_path", help="Path to the bag folder (or .mcap/.db3 file)")
    parser.add_argument("output_csv", help="Path to the output CSV file")
    args = parser.parse_args()

    # Topic to ID mapping configuration (leader/follower)
    topic_mapping = {
        '/leader/gps/fix': 'leader',
        '/follower/gps/fix': 'follower'
    }

    # Bag reader configuration
    # Use 'sqlite3' or 'mcap' depending on your bag format
    storage_options = StorageOptions(uri=args.bag_path, storage_id='sqlite3') 
    converter_options = ConverterOptions(
        input_serialization_format='cdr',
        output_serialization_format='cdr'
    )

    reader = SequentialReader()
    reader.open(storage_options, converter_options)

    # Filter only necessary topics
    filter_options = StorageFilter(topics=list(topic_mapping.keys()))
    reader.set_filter(filter_options)

    print(f"Processing bag: {args.bag_path} -> {args.output_csv}")

    with open(args.output_csv, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        
        # Optional: header
        # writer.writerow(['id', 'timestamp', 'lon', 'lat'])

        count = 0
        while reader.has_next():
            (topic, data, t) = reader.read_next()

            msg = deserialize_message(data, NavSatFix)

            obj_id = topic_mapping[topic]

            ts_millis = (msg.header.stamp.sec * 1000) + (msg.header.stamp.nanosec // 1000000)

            lat = msg.latitude
            lon = msg.longitude

            writer.writerow([obj_id, ts_millis, lon, lat])
            
            count += 1

    print(f"Done! Saved {count} records.")

if __name__ == "__main__":
    main()