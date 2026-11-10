"""
Proportional N-way merge of recording data by stream key.

Stream keys:
  - unit/<SN>  — one stream per unit serial number
  - <namespace> — one stream for every other top-level namespace

Streams are merged so that each stream's rows are spread evenly across
the output at a frequency proportional to its share of the total row
count.  This prevents large same-SN or same-namespace blocks.

The primer block at the top of the file (one row per unique topic) is
detected automatically and preserved as-is.

Within each stream, row order (and therefore per-stream timing) is kept.
"""

import csv
import shutil
from collections import defaultdict

INPUT_FILE = r'C:\dev\mqtt_recorder\recording_demo_v0.2.csv'
BACKUP_FILE = INPUT_FILE + '.distribute.bak'


def get_stream_key(topic):
    """One stream per unit SN; one stream per namespace for everything else."""
    parts = topic.split('/')
    if len(parts) < 4:
        return 'other'
    ns = parts[3]
    if ns == 'unit' and len(parts) >= 5:
        return f'unit/{parts[4]}'   # e.g. unit/02281684
    return ns


def detect_primer_length(rows):
    """Return number of rows in the leading primer block (all unique topics)."""
    seen = set()
    for i, row in enumerate(rows):
        if row[0] in seen:
            return i
        seen.add(row[0])
    return len(rows)


# ── Load ──────────────────────────────────────────────────────────────────────
print(f"Reading {INPUT_FILE} ...")
with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    all_rows = list(csv.reader(f))
print(f"  {len(all_rows):,} total rows")

primer_len = detect_primer_length(all_rows)
primer = all_rows[:primer_len]
data   = all_rows[primer_len:]
print(f"  {primer_len:,} primer rows  |  {len(data):,} data rows")

# ── Split into per-stream streams ────────────────────────────────────────────
streams = defaultdict(list)
for row in data:
    streams[get_stream_key(row[0])].append(row)

total = len(data)
print(f"\nStream breakdown ({len(streams)} streams):")
for key, rows in sorted(streams.items(), key=lambda x: -len(x[1])):
    print(f"  {key:40s}  {len(rows):>7,}  ({100 * len(rows) / total:5.1f}%)")

# ── Assign proportional target positions ─────────────────────────────────────
# Row i (0-indexed) of a stream with n rows is placed at position (i+0.5)*total/n.
# This spreads the stream uniformly across [0, total].
assigned = []
for ns, rows in streams.items():
    n    = len(rows)
    step = total / n
    for i, row in enumerate(rows):
        assigned.append(((i + 0.5) * step, row))

assigned.sort(key=lambda x: x[0])
merged = [row for _, row in assigned]

# ── Verify distribution across 10 equal chunks ───────────────────────────────
chunk = len(merged) // 10
print(f"\nTracker % per tenth of merged data:")
for k in range(10):
    chunk_rows = merged[k * chunk: (k + 1) * chunk]
    tr = sum(1 for r in chunk_rows if '/tracker/' in r[0])
    print(f"  chunk {k+1:2d}: {100 * tr / len(chunk_rows):5.1f}%")

# ── Write ─────────────────────────────────────────────────────────────────────
shutil.copy2(INPUT_FILE, BACKUP_FILE)
print(f"\nBackup written to {BACKUP_FILE}")

with open(INPUT_FILE, 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    for row in primer:
        writer.writerow(row)
    for row in merged:
        writer.writerow(row)

print(f"Done. {primer_len:,} primer + {len(merged):,} data rows written.")
