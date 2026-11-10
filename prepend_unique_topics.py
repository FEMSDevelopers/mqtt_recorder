"""
Prepends a primer block to the CSV: one row per unique topic, with all
non-tracker topics first (in order of first appearance) followed by all
topics containing '/tracker/' (also in order of first appearance).
Primer rows have their inter-message delay set to 0.
"""

import csv
import shutil

INPUT_FILE = r'C:\dev\mqtt_recorder\recording_demo_v0.2.csv'
BACKUP_FILE = INPUT_FILE + '.bak'

# Read all rows
print(f"Reading {INPUT_FILE} ...")
with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    reader = csv.reader(f)
    all_rows = list(reader)

print(f"  {len(all_rows)} rows read")

# Collect first occurrence of each unique topic, preserving file order
seen = {}
for row in all_rows:
    topic = row[0]
    if topic not in seen:
        seen[topic] = row

print(f"  {len(seen)} unique topics found")

# Split into non-tracker and tracker groups, preserving first-seen order
non_tracker = []
tracker = []
for topic, row in seen.items():
    primer_row = row[:5] + ['0']   # zero out the inter-message delay
    if '/tracker/' in topic:
        tracker.append(primer_row)
    else:
        non_tracker.append(primer_row)

primer = non_tracker + tracker
print(f"  Primer: {len(non_tracker)} non-tracker rows, {len(tracker)} tracker rows")

# Back up original then write new file
shutil.copy2(INPUT_FILE, BACKUP_FILE)
print(f"  Backup written to {BACKUP_FILE}")

with open(INPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for row in primer:
        writer.writerow(row)
    for row in all_rows:
        writer.writerow(row)

print(f"Done. New file: {len(primer)} primer rows + {len(all_rows)} original rows = {len(primer) + len(all_rows)} total")
