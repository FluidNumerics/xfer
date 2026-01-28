#!/usr/bin/env python3
"""
sum.py

Reads newline-delimited JSON objects produced from rclone listings (e.g., `rclone lsjson ... | jq -c '.[]'`)
and totals up the data volume. Also prints an estimated transfer time table for a range of transfer rates,
AND prints an ASCII histogram of file-size distribution.

Supports:
- NDJSON (one JSON object per line)
- A single JSON array (the default output of `rclone lsjson`)

Histogram:
- Default bins are log2-spaced (powers of two) from 1 KiB up to 1 TiB.
- Counts + bytes per bin are shown with an ASCII bar.
- You can override with --hist-min, --hist-max, --hist-bins.

Examples:
  rclone lsjson s3:bucket/prefix --recursive --fast-list | jq -c '.[]' > listing.jsonl
  ./sum_rclone_jsonl.py listing.jsonl --min-rate 5Gbps --max-rate 40Gbps

  ./sum_rclone_jsonl.py listing.jsonl --min-rate 1Gbps --max-rate 10Gbps --hist-bins 40

  cat listing.jsonl | ./sum_rclone_jsonl.py - --min-rate 1Gbps --max-rate 10Gbps
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------
# Human formatting helpers
# -----------------------------
def human_bytes(n: int) -> str:
    # Binary units for data size (GiB, TiB, ...)
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    x = float(n)
    for u in units:
        if x < 1024.0 or u == units[-1]:
            if u == "B":
                return f"{int(x)} {u}"
            return f"{x:.2f} {u}"
        x /= 1024.0
    return f"{x:.2f} PiB"


def human_seconds(seconds: float) -> str:
    if seconds < 0 or math.isinf(seconds) or math.isnan(seconds):
        return "n/a"
    sec = int(round(seconds))
    days, rem = divmod(sec, 86400)
    hours, rem = divmod(rem, 3600)
    mins, secs = divmod(rem, 60)

    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or (days and (mins or secs)):
        parts.append(f"{hours}h")
    if mins or ((days or hours) and secs):
        parts.append(f"{mins}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


# -----------------------------
# Rate parsing
# -----------------------------
_RATE_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z/]+)\s*$")


def parse_rate_to_Bps(s: str) -> float:
    """
    Parse a transfer rate string to Bytes/sec.

    Accepted examples:
      - 10Gbps, 1.5Gb/s, 800Mbps
      - 100MB/s, 250MiB/s, 2GB/s, 1GiB/s
      - 1000KB/s, 1000KiB/s

    Notes:
      - 'b' = bits, 'B' = bytes
      - SI (K, M, G, T) uses 10^3
      - IEC (Ki, Mi, Gi, Ti) uses 2^10
    """
    m = _RATE_RE.match(s)
    if not m:
        raise ValueError(f"Could not parse rate: {s!r}")

    value = float(m.group(1))
    unit = m.group(2)

    unit = unit.replace("ps", "/s").replace("p/s", "/s")
    unit = unit.replace("PerSec", "/s")
    unit = unit.strip()

    # Normalize common variants
    unit = unit.replace("Gb/s", "Gbps").replace("Mb/s", "Mbps").replace("Kb/s", "Kbps")
    unit = unit.replace("GB/s", "GBps").replace("MB/s", "MBps").replace("KB/s", "KBps")
    unit = (
        unit.replace("GiB/s", "GiBps")
        .replace("MiB/s", "MiBps")
        .replace("KiB/s", "KiBps")
    )
    unit = unit.replace("/s", "ps")

    is_bits = unit.endswith("bps") and not unit.endswith("Bps")
    is_bytes = unit.endswith("Bps")

    if not (is_bits or is_bytes):
        raise ValueError(
            f"Rate unit must be bits or bytes per second (e.g., Gbps or MB/s). Got {unit!r}"
        )

    base_unit = unit[:-3]  # remove bps/Bps

    si = {"": 1.0, "K": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "P": 1e15}
    iec = {
        "Ki": 1024.0,
        "Mi": 1024.0**2,
        "Gi": 1024.0**3,
        "Ti": 1024.0**4,
        "Pi": 1024.0**5,
    }

    if base_unit in si:
        scale = si[base_unit]
    elif base_unit in iec:
        scale = iec[base_unit]
    else:
        raise ValueError(
            f"Unrecognized prefix in rate unit: {base_unit!r} from {unit!r}"
        )

    # Convert to bytes/sec
    bits_per_sec = value * scale if is_bits else (value * scale * 8.0)
    return bits_per_sec / 8.0


def format_rate_Bps(Bps: float, prefer: str = "Gbps") -> str:
    if prefer.lower().startswith("g"):
        gbps = (Bps * 8.0) / 1e9
        return f"{gbps:.2f} Gbps"
    mbps = Bps / 1e6
    return f"{mbps:.2f} MB/s"


# -----------------------------
# Input reading
# -----------------------------
def iter_json_objects_from_file(fp) -> Iterable[Dict[str, Any]]:
    """
    Yield JSON objects from fp supporting:
    - NDJSON (one object per line)
    - A single JSON array
    """
    # Peek first non-whitespace char (best-effort)
    if hasattr(fp, "seek") and hasattr(fp, "tell"):
        pos = fp.tell()
        first = ""
        while True:
            ch = fp.read(1)
            if not ch:
                break
            if not ch.isspace():
                first = ch
                break
        fp.seek(pos)

        if first == "[":
            data = json.load(fp)
            if not isinstance(data, list):
                raise ValueError("Expected a JSON array from rclone lsjson")
            for item in data:
                if isinstance(item, dict):
                    yield item
            return

    # Fallback NDJSON
    for line in fp:
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="replace")
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            yield obj


def extract_size_bytes(obj: Dict[str, Any]) -> Optional[int]:
    if obj.get("IsDir") is True or obj.get("isdir") is True:
        return None
    size = obj.get("Size", obj.get("size"))
    if size is None:
        return None
    try:
        return int(size)
    except Exception:
        return None


@dataclass
class Totals:
    objects: int
    bytes_total: int


def compute_totals_and_sizes(
    objs: Iterable[Dict[str, Any]],
) -> Tuple[Totals, List[int]]:
    count = 0
    total = 0
    sizes: List[int] = []
    for o in objs:
        sz = extract_size_bytes(o)
        if sz is None:
            continue
        total += sz
        count += 1
        sizes.append(sz)
    return Totals(objects=count, bytes_total=total), sizes


# -----------------------------
# Time table
# -----------------------------
def rate_samples(min_Bps: float, max_Bps: float, rows: int = 7) -> List[float]:
    if min_Bps <= 0 or max_Bps <= 0:
        return []
    if max_Bps < min_Bps:
        min_Bps, max_Bps = max_Bps, min_Bps
    if math.isclose(min_Bps, max_Bps):
        return [min_Bps]
    samples = []
    for i in range(rows):
        t = i / (rows - 1)
        samples.append(min_Bps * ((max_Bps / min_Bps) ** t))
    return samples


def print_time_table(
    bytes_total: int, min_rate: str, max_rate: str, prefer_units: str = "Gbps"
) -> None:
    min_Bps = parse_rate_to_Bps(min_rate)
    max_Bps = parse_rate_to_Bps(max_rate)
    samples = rate_samples(min_Bps, max_Bps, rows=7)

    print("\nEstimated transfer times")
    print(f"Total data: {human_bytes(bytes_total)} ({bytes_total} bytes)")
    print(f"Rate range: {min_rate} .. {max_rate}\n")

    print("| Assumed rate | Time estimate |")
    print("|---:|---:|")
    for r in samples:
        secs = bytes_total / r if r > 0 else float("inf")
        print(f"| {format_rate_Bps(r, prefer=prefer_units)} | {human_seconds(secs)} |")

    print("\nRange summary")
    print(
        f"- Slow end ({format_rate_Bps(min_Bps, prefer=prefer_units)}): {human_seconds(bytes_total / min_Bps)}"
    )
    print(
        f"- Fast end ({format_rate_Bps(max_Bps, prefer=prefer_units)}): {human_seconds(bytes_total / max_Bps)}"
    )


# -----------------------------
# ASCII Histogram
# -----------------------------
def logspace_edges(min_bytes: int, max_bytes: int, bins: int) -> List[float]:
    """
    Log-spaced edges from min_bytes to max_bytes inclusive.
    """
    if min_bytes <= 0:
        min_bytes = 1
    if max_bytes <= min_bytes:
        max_bytes = min_bytes + 1
    lo = math.log10(min_bytes)
    hi = math.log10(max_bytes)
    step = (hi - lo) / bins
    return [10 ** (lo + i * step) for i in range(bins + 1)]


def default_pow2_edges(min_bytes: int, max_bytes: int) -> List[int]:
    """
    Power-of-two bin edges (base 2) spanning [min_bytes, max_bytes].
    """
    if min_bytes <= 1:
        min_bytes = 1
    # next power-of-two <= min_bytes
    lo_pow = 2 ** int(math.floor(math.log2(min_bytes)))
    hi_pow = 2 ** int(math.ceil(math.log2(max_bytes)))
    edges = []
    x = lo_pow
    while x < hi_pow:
        edges.append(x)
        x *= 2
    edges.append(hi_pow)
    return edges


def histogram_counts(
    sizes: List[int], edges: List[float]
) -> Tuple[List[int], List[int]]:
    """
    Returns (counts, bytes_per_bin) for bins defined by edges.
    Bin i covers [edges[i], edges[i+1]) except last is [.., ..]
    """
    k = len(edges) - 1
    counts = [0] * k
    bytes_bin = [0] * k
    for sz in sizes:
        # find bin (linear scan is fine for small k; k ~ 20-60)
        # edges are increasing
        j = None
        for i in range(k):
            if i < k - 1:
                if edges[i] <= sz < edges[i + 1]:
                    j = i
                    break
            else:
                if edges[i] <= sz <= edges[i + 1]:
                    j = i
                    break
        if j is None:
            if sz < edges[0]:
                j = 0
            else:
                j = k - 1
        counts[j] += 1
        bytes_bin[j] += sz
    return counts, bytes_bin


def ascii_bar(value: int, max_value: int, width: int = 40) -> str:
    if max_value <= 0:
        return ""
    n = int(round((value / max_value) * width))
    return "█" * n


def print_histogram(
    sizes: List[int],
    *,
    hist_min: str = "1KiB",
    hist_max: str = "1TiB",
    hist_bins: int = 0,
    hist_width: int = 40,
    mode: str = "pow2",
) -> None:
    """
    Print an ASCII histogram of file sizes.

    mode:
      - "pow2": bins are powers-of-two (nice for file sizes); ignores hist_bins
      - "log": log10-spaced bins using hist_bins
    """
    if not sizes:
        print("\nFile size histogram\n(no files)")
        return

    # Parse min/max as bytes; accept inputs like 1KiB, 10MiB, 2GiB, 1TiB
    def parse_bytes(s: str) -> int:
        s = s.strip()
        m = re.match(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)\s*$", s)
        if not m:
            raise ValueError(f"Could not parse byte size: {s!r}")
        val = float(m.group(1))
        unit = m.group(2)

        unit_map = {
            "B": 1,
            "KB": 1000,
            "MB": 1000**2,
            "GB": 1000**3,
            "TB": 1000**4,
            "PB": 1000**5,
            "KiB": 1024,
            "MiB": 1024**2,
            "GiB": 1024**3,
            "TiB": 1024**4,
            "PiB": 1024**5,
        }
        if unit not in unit_map:
            raise ValueError(
                f"Unknown unit {unit!r}. Use B, KB, MB, GB, TB, KiB, MiB, GiB, TiB."
            )
        return int(val * unit_map[unit])

    min_b = parse_bytes(hist_min)
    max_b = parse_bytes(hist_max)

    # Clamp to observed range if user picked something too narrow
    obs_min = max(1, min(sizes))
    obs_max = max(sizes)

    # If user left defaults and data is outside, expand automatically
    if min_b > obs_min:
        min_b = 2 ** int(math.floor(math.log2(obs_min)))
    if max_b < obs_max:
        max_b = 2 ** int(math.ceil(math.log2(obs_max)))

    if mode == "log":
        if hist_bins <= 0:
            hist_bins = 30
        edges = logspace_edges(min_b, max_b, hist_bins)
    else:
        edges_int = default_pow2_edges(min_b, max_b)
        edges = [float(x) for x in edges_int]

    counts, bytes_bin = histogram_counts(sizes, edges)
    max_count = max(counts) if counts else 0

    total_files = len(sizes)
    total_bytes = sum(sizes)

    print("\nFile size histogram")
    print(f"Files: {total_files}  Total: {human_bytes(total_bytes)}")
    print(f"Observed: min={human_bytes(obs_min)}  max={human_bytes(obs_max)}")
    print(
        f"Bins: {len(edges)-1}  Mode: {mode}  Range: {human_bytes(int(edges[0]))} .. {human_bytes(int(edges[-1]))}\n"
    )

    print("| Size range | Files | % files | Bytes in bin | % bytes | Histogram |")
    print("|---|---:|---:|---:|---:|---|")

    for i in range(len(edges) - 1):
        lo = int(edges[i])
        hi = int(edges[i + 1])
        c = counts[i]
        b = bytes_bin[i]
        pf = (100.0 * c / total_files) if total_files else 0.0
        pb = (100.0 * b / total_bytes) if total_bytes else 0.0
        bar = ascii_bar(c, max_count, width=hist_width)
        # show inclusive/exclusive cleanly
        label = (
            f"[{human_bytes(lo)}, {human_bytes(hi)})"
            if i < (len(edges) - 2)
            else f"[{human_bytes(lo)}, {human_bytes(hi)}]"
        )
        print(f"| {label} | {c} | {pf:5.1f}% | {human_bytes(b)} | {pb:5.1f}% | {bar} |")


# -----------------------------
# Main
# -----------------------------
def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sum rclone NDJSON/lsjson, estimate time range, and print size histogram."
    )
    ap.add_argument("input", help="Path to NDJSON or JSON array file, or '-' for stdin")
    ap.add_argument(
        "--min-rate",
        required=True,
        help="Minimum expected transfer rate (e.g., 5Gbps, 200MB/s)",
    )
    ap.add_argument(
        "--max-rate",
        required=True,
        help="Maximum expected transfer rate (e.g., 40Gbps, 1GB/s)",
    )
    ap.add_argument(
        "--prefer-units",
        choices=["Gbps", "MBps"],
        default="Gbps",
        help="How to display rates in the time table (default: Gbps)",
    )

    # Histogram options
    ap.add_argument(
        "--hist", action="store_true", help="Print file size histogram (default: on)"
    )
    ap.add_argument(
        "--no-hist", action="store_true", help="Disable file size histogram"
    )
    ap.add_argument(
        "--hist-mode",
        choices=["pow2", "log"],
        default="pow2",
        help="Histogram binning mode",
    )
    ap.add_argument(
        "--hist-min", default="1KiB", help="Histogram minimum edge (e.g., 1KiB, 1MiB)"
    )
    ap.add_argument(
        "--hist-max", default="1TiB", help="Histogram maximum edge (e.g., 1GiB, 10TiB)"
    )
    ap.add_argument(
        "--hist-bins",
        type=int,
        default=0,
        help="Number of bins (only for --hist-mode log)",
    )
    ap.add_argument("--hist-width", type=int, default=40, help="ASCII bar width")

    args = ap.parse_args()

    do_hist = True
    if args.no_hist:
        do_hist = False
    elif args.hist:
        do_hist = True

    if args.input == "-":
        objs = iter_json_objects_from_file(sys.stdin)
        totals, sizes = compute_totals_and_sizes(objs)
    else:
        with open(args.input, "r", encoding="utf-8") as fp:
            objs = iter_json_objects_from_file(fp)
            totals, sizes = compute_totals_and_sizes(objs)

    print(f"Objects counted: {totals.objects}")
    print(f"Total bytes:     {totals.bytes_total}")
    print(f"Total size:      {human_bytes(totals.bytes_total)}")

    print_time_table(
        totals.bytes_total, args.min_rate, args.max_rate, prefer_units=args.prefer_units
    )

    if do_hist:
        print_histogram(
            sizes,
            hist_min=args.hist_min,
            hist_max=args.hist_max,
            hist_bins=args.hist_bins,
            hist_width=args.hist_width,
            mode=args.hist_mode,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
