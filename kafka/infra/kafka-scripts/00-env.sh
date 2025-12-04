#!/usr/bin/env bash

# Kafka 기본 환경 변수

# 브로커 노드 리스트
BROKER="kafka-1:9092,kafka-2:9092,kafka-3:9092"

# 컨테이너 내부 Kafka CLI binary 위치
KAFKA_BIN="/opt/kafka/bin"

# Kafka 스크립트 로깅 경로
LOG_DIR="/opt/kafka/logs"

# Kafka 스크립트 로깅 경로
LOG_DIR="/scripts/logs"
mkdir -p $LOG_DIR

# 기본 replication / partition 값
DEFAULT_PARTITIONS=3
DEFAULT_REPLICATION=3

# 기본 생성 Topic
declare -A DEFAULT_TOPICS=(
  ["order-event"]=6
  ["payment-event"]=3
  ["product-event"]=3
  ["settlement-event"]=3
  ["deposit-event"]=3
)

# 시간 포맷
NOW=$(date '+%Y-%m-%d %H:%M:%S')

# 로그 출력 함수
log() {
  echo "[$NOW] $1"
}