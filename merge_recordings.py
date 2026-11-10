"""
merge_recordings.py

Merges a new MQTT recording CSV into an existing one, remapping the new
recording's timestamps to fit within the existing recording's time range,
then recalculating relative delays for correct mqtt_recorder playback.

Usage:
    python merge_recordings.py <existing_csv> <new_csv> <output_csv>

CSV column format (no header):
    [0] topic
    [1] payload (JSON)
    [2] qos
    [3] retain
    [4] absolute_timestamp (Unix epoch float)
    [5] relative_delay (seconds float — drives time.sleep() during playback)
"""

import csv
import sys


def read_csv(path):
    rows = []
    with open(path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            rows.append(row)
    return rows


def time_range(rows):
    timestamps = [float(r[4]) for r in rows if len(r) > 4]
    return min(timestamps), max(timestamps)


def normalize_topic(topic):
    """Strip site/unit serial numbers based on element count for reporting."""
    parts = topic.split('/')
    n = len(parts)
    if n >= 8:  # unit-level: strip parts[2] (site serial) and parts[4] (unit serial)
        parts[2] = '+'
        parts[4] = '+'
    elif n >= 6:  # site-level: strip parts[2] (site serial)
        parts[2] = '+'
    return '/'.join(parts)


def main():
    if len(sys.argv) != 4:
        print("Usage: python merge_recordings.py <existing_csv> <new_csv> <output_csv>")
        sys.exit(1)

    existing_path, new_path, output_path = sys.argv[1], sys.argv[2], sys.argv[3]

    print(f"Reading existing: {existing_path}")
    existing_rows = read_csv(existing_path)
    print(f"Reading new:      {new_path}")
    new_rows = read_csv(new_path)

    existing_min, existing_max = time_range(existing_rows)
    new_min, new_max = time_range(new_rows)
    existing_span = existing_max - existing_min
    new_span = new_max - new_min

    print(f"\nExisting time range: {existing_span:.1f}s  ({len(existing_rows)} rows)")
    print(f"New time range:      {new_span:.1f}s  ({len(new_rows)} rows)")

    # Remap new recording timestamps into existing time range
    scale = existing_span / new_span if new_span > 0 else 1.0
    for row in new_rows:
        if len(row) > 4:
            orig_ts = float(row[4])
            row[4] = str(existing_min + (orig_ts - new_min) * scale)

    # Merge and sort by absolute timestamp
    combined = existing_rows + new_rows
    combined.sort(key=lambda r: float(r[4]) if len(r) > 4 else 0.0)

    # Recalculate relative delays
    for i, row in enumerate(combined):
        if len(row) > 5:
            if i == 0:
                row[5] = '0.0'
            else:
                delay = float(combined[i][4]) - float(combined[i - 1][4])
                row[5] = str(max(0.0, delay))

    # Validate: report any topic types unique to new recording
    existing_normalized = {normalize_topic(r[0]) for r in existing_rows if r}
    new_normalized = {normalize_topic(r[0]) for r in new_rows if r}
    new_only = new_normalized - existing_normalized
    if new_only:
        print(f"\nTopics in new recording with no match in existing ({len(new_only)}):")
        for t in sorted(new_only)[:20]:
            print(f"  {t}")
        if len(new_only) > 20:
            print(f"  ... and {len(new_only) - 20} more")

    # Write output
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(combined)

    print(f"\nOutput: {output_path}")
    print(f"Total rows: {len(combined)}  (existing: {len(existing_rows)}, new: {len(new_rows)})")
    print(f"Time range: {existing_span:.1f}s")


if __name__ == '__main__':
    main()
