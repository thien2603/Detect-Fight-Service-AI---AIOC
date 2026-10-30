"""Đọc log có sẵn để so sánh độ trễ, không chạy model hoặc publish MQTT.

Chạy: .venv/bin/python measure_latency.py --window 60 --save-name latency_after.json
--instrumented chỉ lấy tin có frame_seq của phiên chạy mới nhất, bỏ 5s khởi động.
Đọc tối đa 16 MiB
log gần nhất kể cả archive, không tạo video/snapshot hay file log vô hạn.
"""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import statistics


def summary(values):
    if not values:
        return None
    values = sorted(values)
    return {"count": len(values), "min": round(values[0], 2),
            "median": round(statistics.median(values), 2),
            "p95": round(values[int(.95 * (len(values) - 1))], 2), "max": round(values[-1], 2)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--window", type=float, default=60)
    parser.add_argument("--instrumented", action="store_true")
    parser.add_argument("--save-name")
    args = parser.parse_args()
    output = Path(__file__).resolve().parent / "output"
    files = sorted(output.glob("log.jsonl*"), key=lambda p: p.stat().st_mtime, reverse=True)
    chunks, remaining = [], 16 * 1024 * 1024
    for file in files:
        if not file.is_file() or file.is_symlink() or remaining <= 0:
            continue
        size = file.stat().st_size
        with file.open("rb") as stream:
            stream.seek(max(0, size - remaining))
            chunk = stream.read(remaining)
        remaining -= len(chunk)
        chunks.append(chunk)
    rows = []
    for chunk in reversed(chunks):
        for line in chunk.splitlines():
            try:
                rows.append(json.loads(line))
            except (ValueError, UnicodeDecodeError):
                pass
    sent = [r for r in rows if r.get("kind") == "bbox_send"
            and (not args.instrumented or "frame_seq" in r)]
    if args.instrumented:
        starts = [datetime.fromisoformat(r["time"]).timestamp() for r in rows if r.get("kind") == "starting"]
        if starts:
            sent = [r for r in sent if r["payload"]["timestamp"] >= max(starts) + 5]
    if not sent:
        raise SystemExit("Chưa có bbox_send phù hợp")
    end = max(r["payload"]["timestamp"] for r in sent)
    sent = sorted([r for r in sent if r["payload"]["timestamp"] >= end - args.window],
                  key=lambda r: r["payload"]["timestamp"])
    stamps = [r["payload"]["timestamp"] for r in sent]
    sent_by_stamp = {r["payload"]["timestamp"]: r for r in sent}
    echoed, roundtrips = set(), []
    for row in rows:
        if row.get("kind") != "bbox_received_from_broker" or not row.get("correlated"):
            continue
        stamp = row.get("payload", {}).get("timestamp")
        if stamp in sent_by_stamp:
            echoed.add(stamp)
            roundtrips.append(row.get("broker_roundtrip_ms") or
                             (datetime.fromisoformat(row["time"]).timestamp() - stamp) * 1000)
    span = stamps[-1] - stamps[0]
    intervals = [(b - a) * 1000 for a, b in zip(stamps, stamps[1:])]
    sequences = [r["frame_seq"] for r in sent if "frame_seq" in r]
    result = {"checked_at": datetime.now(timezone.utc).isoformat(), "requested_window_seconds": args.window,
              "observed_span_seconds": round(span, 2), "bbox_messages": len(sent), "broker_echoes": len(echoed),
              "bbox_hz": round((len(sent) - 1) / span, 2) if span else 0,
              "bbox_age_at_publish_ms": summary([r["result_age_ms"] for r in sent]),
              "capture_to_inference_ms": summary([r["capture_to_inference_ms"] for r in sent if "capture_to_inference_ms" in r]),
              "inference_ms": summary([r["inference_ms"] for r in sent if "inference_ms" in r]),
              "publisher_wait_ms": summary([r["publisher_wait_ms"] for r in sent if "publisher_wait_ms" in r]),
              "publish_interval_ms": summary(intervals), "broker_echo_ms": summary(roundtrips),
              "intervals_over_350ms": sum(v > 350 for v in intervals),
              "duplicate_frame_results": len(sequences) - len(set(sequences)) if sequences else None,
              "schema_invalid": sum(not r.get("schema_valid") for r in sent),
              "publish_errors": sum(r.get("publish_rc") != 0 for r in sent)}
    if args.save_name:
        (output / Path(args.save_name).name).write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2), flush=True)


if __name__ == "__main__":
    main()
