import os

KAFKA_BROKER       = os.getenv("KAFKA_BROKER",       "dx-kafka-cluster-kafka-bootstrap.observability.svc.cluster.local:9092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",         "datacenter.metrics")
KAFKA_REPLAY_TOPIC = os.getenv("KAFKA_REPLAY_TOPIC",  "datacenter.metrics.replay")
KAFKA_EVENT_TOPIC  = os.getenv("KAFKA_EVENT_TOPIC",   "datacenter.metrics.event")
KAFKA_REPLAY_EVENT_TOPIC = os.getenv("KAFKA_REPLAY_EVENT_TOPIC", "datacenter.metrics.replay.event")
KAFKA_NODE_STATE_TOPIC = os.getenv("KAFKA_NODE_STATE_TOPIC", "datacenter.metrics.node-state.events")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID",      "query-server")

TRINO_HOST    = os.getenv("TRINO_HOST",    "10.32.161.108")
TRINO_PORT    = int(os.getenv("TRINO_PORT", "31010"))
TRINO_USER    = os.getenv("TRINO_USER",    "trino")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "bronze_cat")
TRINO_SCHEMA  = os.getenv("TRINO_SCHEMA",  "infra")

# LiveCache: 박스당 최근 N개 메시지 보관
LIVE_CACHE_SIZE = int(os.getenv("LIVE_CACHE_SIZE", "10"))
