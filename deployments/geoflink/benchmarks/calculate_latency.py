import json
import sys
import numpy as np

FROM = "ts_0"
TO = "ts_3"


def load_data(file_path):
    """Load collected latency data from a JSON file."""
    with open(file_path, "r") as f:
        return json.load(f)


def compute_latency_stats(data):
    """Compute and print latency statistics from collected data."""
    latencies = []

    for record in data.get("data", []):
        try:
            ts = record.get(FROM)
            ts_received = record.get(TO)

            if ts is None or ts_received is None:
                continue

            latency = ts_received - ts
            latencies.append(latency)

        except Exception:
            continue

    if not latencies:
        print("No valid latency data found.")
        return

    arr = np.array(latencies)

    avg = np.mean(arr)
    median = np.percentile(arr, 50)
    p99 = np.percentile(arr, 99)
    std = np.std(arr)

    print("\n=== LATENCY STATS (ms) ===")
    print(f"Count   : {len(arr)}")
    print(f"Average : {avg:.3f}")
    print(f"P50     : {median:.3f}")
    print(f"P99     : {p99:.3f}")
    print(f"Std Dev : {std:.3f}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python latency_stats.py <results.json>")
        sys.exit(1)

    file_path = sys.argv[1]
    data = load_data(file_path)
    compute_latency_stats(data)
