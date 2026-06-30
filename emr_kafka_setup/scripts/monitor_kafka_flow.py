#!/usr/bin/env python3
"""Observe Kafka quorum, topic flow and consumer lag without consuming events."""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


TOPICS = (
    "raw_youtube_chat",
    "nlp_stream_results",
    "alerts_polarization",
    "nlp_batch_results",
)


def parse_args():
    parser = argparse.ArgumentParser(description="Monitor Kafka pipeline health.")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Print one snapshot and exit.")
    mode.add_argument("--follow", action="store_true", help="Write snapshots continuously.")
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--project-home", default="/home/hadoop/bigdata-kafka")
    parser.add_argument("--history-max-bytes", type=int, default=10 * 1024 * 1024)
    return parser.parse_args()


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def run(command, timeout=15):
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except Exception as exc:
        return 1, "", str(exc)


def atomic_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def rotate_history(path, max_bytes):
    if path.exists() and path.stat().st_size >= max_bytes:
        rotated = path.with_suffix(path.suffix + ".1")
        rotated.unlink(missing_ok=True)
        path.replace(rotated)


class KafkaFlowMonitor:
    def __init__(self, args):
        self.args = args
        self.kafka_home = Path(os.environ.get("KAFKA_HOME", "/home/hadoop/kafka"))
        self.project_home = Path(args.project_home)
        self.logs = self.project_home / "logs"
        self.snapshot_path = self.logs / "kafka_flow_health.json"
        self.history_path = self.logs / "kafka_flow_history.jsonl"
        self.previous_offsets = {}
        self.previous_at = None

    def kafka_tool(self, name):
        return str(self.kafka_home / "bin" / name)

    def quorum(self):
        command = [
            self.kafka_tool("kafka-metadata-quorum.sh"),
            "--bootstrap-server",
            self.args.bootstrap_server,
            "describe",
            "--status",
        ]
        code, stdout, stderr = run(command)
        fields = {}
        for line in stdout.splitlines():
            if ":" in line:
                key, value = line.split(":", 1)
                fields[key.strip().lower().replace(" ", "_")] = value.strip()
        voters_raw = fields.get("current_voters", fields.get("currentvoters", ""))
        voter_ids = sorted(set(re.findall(r'(?:"id"\s*:\s*|replicaId=)(\d+)', voters_raw)))
        if not voter_ids:
            voter_ids = [item.strip() for item in voters_raw.strip("[]").split(",") if item.strip()]
        voters = voter_ids
        leader = fields.get("leaderid", fields.get("leader_id", ""))
        return {
            "healthy": code == 0 and len(voters) == 3 and bool(leader),
            "leader_id": leader,
            "voters": voters,
            "voter_count": len(voters),
            "high_watermark": fields.get("highwatermark", fields.get("high_watermark", "")),
            "error": stderr if code else "",
        }

    def topic_offsets(self):
        topics = {}
        totals = {}
        for topic in TOPICS:
            command = [
                self.kafka_tool("kafka-run-class.sh"),
                "kafka.tools.GetOffsetShell",
                "--broker-list",
                self.args.bootstrap_server,
                "--topic",
                topic,
            ]
            code, stdout, stderr = run(command)
            partitions = {}
            total = 0
            for line in stdout.splitlines():
                parts = line.rsplit(":", 2)
                if len(parts) != 3:
                    continue
                try:
                    partition, offset = int(parts[1]), int(parts[2])
                except ValueError:
                    continue
                partitions[str(partition)] = offset
                total += offset
            topics[topic] = {
                "total": total,
                "partitions": partitions,
                "error": stderr if code else "",
            }
            totals[topic] = total
        return topics, totals

    def topic_health(self):
        command = [
            self.kafka_tool("kafka-topics.sh"),
            "--bootstrap-server",
            self.args.bootstrap_server,
            "--describe",
        ]
        code, stdout, stderr = run(command)
        topic_stats = {}
        under_replicated = 0
        offline = 0
        partition_pattern = re.compile(
            r"Topic:\s+(\S+).*Partition:\s+(\d+).*Leader:\s+(-?\d+).*Replicas:\s+([0-9,]+).*Isr:\s*([0-9,]*)"
        )
        for line in stdout.splitlines():
            match = partition_pattern.search(line)
            if not match:
                continue
            topic, _, leader, replicas_raw, isr_raw = match.groups()
            replicas = [item for item in replicas_raw.split(",") if item]
            isr = [item for item in isr_raw.split(",") if item]
            stats = topic_stats.setdefault(
                topic,
                {"partitions": 0, "replication_factor": len(replicas), "under_replicated": 0, "offline": 0},
            )
            stats["partitions"] += 1
            if len(isr) < len(replicas):
                stats["under_replicated"] += 1
                under_replicated += 1
            if leader == "-1":
                stats["offline"] += 1
                offline += 1
        return {
            "healthy": code == 0 and under_replicated == 0 and offline == 0,
            "under_replicated_partitions": under_replicated,
            "offline_partitions": offline,
            "topics": topic_stats,
            "error": stderr if code else "",
        }

    def consumer_lag(self):
        command = [
            self.kafka_tool("kafka-consumer-groups.sh"),
            "--bootstrap-server",
            self.args.bootstrap_server,
            "--describe",
            "--all-groups",
        ]
        code, stdout, stderr = run(command)
        groups = {}
        for line in stdout.splitlines():
            fields = line.split()
            if len(fields) < 6 or fields[0] == "GROUP":
                continue
            group, topic = fields[0], fields[1]
            try:
                lag = int(fields[5]) if fields[5] != "-" else 0
            except ValueError:
                continue
            entry = groups.setdefault(group, {"lag": 0, "topics": {}})
            entry["lag"] += lag
            entry["topics"][topic] = entry["topics"].get(topic, 0) + lag
        return {
            "total_lag": sum(item["lag"] for item in groups.values()),
            "groups": groups,
            "error": stderr if code and not groups else "",
        }

    def broker_errors(self):
        error_log = self.logs / "kafka-server.err"
        if not error_log.exists():
            return []
        try:
            lines = error_log.read_text(encoding="utf-8", errors="replace").splitlines()[-300:]
        except OSError:
            return []
        return [line[-500:] for line in lines if re.search(r"\b(ERROR|FATAL)\b", line)][-10:]

    def process_health(self):
        pid_path = self.logs / "kafka.pid"
        producer_pids = run(["pgrep", "-f", "produce_youtube_chat_from_s3.py"], timeout=3)[1].splitlines()
        broker_running = False
        if pid_path.exists():
            pid = pid_path.read_text(encoding="utf-8").strip()
            broker_running = bool(pid) and run(["kill", "-0", pid], timeout=3)[0] == 0
        return {
            "local_broker_running": broker_running,
            "producer_running": bool(producer_pids),
            "producer_processes": len(producer_pids),
        }

    def session_config(self):
        path = self.logs / "session_config.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def snapshot(self):
        now_monotonic = time.monotonic()
        topics, totals = self.topic_offsets()
        elapsed = now_monotonic - self.previous_at if self.previous_at is not None else 0
        rates = {}
        for topic, total in totals.items():
            previous = self.previous_offsets.get(topic, total)
            rates[topic] = round(max(total - previous, 0) / elapsed, 3) if elapsed > 0 else 0.0
        self.previous_offsets = totals
        self.previous_at = now_monotonic

        quorum = self.quorum()
        replication = self.topic_health()
        lag = self.consumer_lag()
        processes = self.process_health()
        session = self.session_config()
        expected = int(session.get("expected_messages", 0) or 0)
        session["complete"] = bool(
            expected > 0
            and totals.get("raw_youtube_chat", 0) >= expected
            and not processes["producer_running"]
        )
        errors = self.broker_errors()
        healthy = (
            quorum["healthy"]
            and replication["healthy"]
            and processes["local_broker_running"]
        )
        return {
            "ok": True,
            "status": "healthy" if healthy else "degraded",
            "generated_at": utc_now(),
            "bootstrap_server": self.args.bootstrap_server,
            "quorum": quorum,
            "replication": replication,
            "topics": topics,
            "rates_per_second": rates,
            "consumer_lag": lag,
            "processes": processes,
            "session": session,
            "recent_errors": errors,
        }

    def persist(self, payload):
        atomic_json(self.snapshot_path, payload)
        rotate_history(self.history_path, self.args.history_max_bytes)
        with self.history_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")


def main():
    args = parse_args()
    monitor = KafkaFlowMonitor(args)
    if args.once or not args.follow:
        payload = monitor.snapshot()
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if payload["status"] == "healthy" else 1

    while True:
        try:
            payload = monitor.snapshot()
            monitor.persist(payload)
            print(
                f"{payload['generated_at']} status={payload['status']} "
                f"raw={payload['topics']['raw_youtube_chat']['total']} "
                f"out={payload['topics']['nlp_stream_results']['total']}",
                flush=True,
            )
        except KeyboardInterrupt:
            return 0
        except Exception as exc:
            print(f"{utc_now()} monitor_error={exc}", file=sys.stderr, flush=True)
        time.sleep(max(args.interval, 1.0))


if __name__ == "__main__":
    raise SystemExit(main())
