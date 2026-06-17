#!/usr/bin/env bash
set -euo pipefail

PROJECT_HOME="${PROJECT_HOME:-/home/hadoop/bigdata-kafka}"
SRC_DIR="$PROJECT_HOME/flink/jobs"
BUILD_DIR="$PROJECT_HOME/flink/build"
JAR_OUT="$SRC_DIR/flink-streaming-jobs.jar"

mkdir -p "$BUILD_DIR/classes" "$BUILD_DIR/fat"
rm -rf "$BUILD_DIR/classes"/* "$BUILD_DIR/fat"/*

FLINK_CP="/usr/lib/flink/lib/*:/usr/lib/flink/opt/*"
KAFKA_CP="/home/hadoop/kafka/libs/*"

javac -source 1.8 -target 1.8 -cp "$FLINK_CP:$KAFKA_CP" \
  -d "$BUILD_DIR/classes" \
  "$SRC_DIR/FlinkKafkaStreamingJobs.java"

cd "$BUILD_DIR/fat"
jar xf /home/hadoop/kafka/libs/kafka-clients-3.6.2.jar
jar xf /home/hadoop/kafka/libs/lz4-java-1.8.0.jar
jar xf /home/hadoop/kafka/libs/snappy-java-1.1.10.5.jar
jar xf /home/hadoop/kafka/libs/zstd-jni-1.5.5-1.jar
rm -f META-INF/*.SF META-INF/*.DSA META-INF/*.RSA 2>/dev/null || true
cp -R "$BUILD_DIR/classes"/* .
jar cfe "$JAR_OUT" FlinkKafkaStreamingJobs .

echo "$JAR_OUT"

