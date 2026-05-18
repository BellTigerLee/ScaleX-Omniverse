import os

# Kafka
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "dx-kafka-cluster-kafka-bootstrap.observability.svc.cluster.local:9092")
KAFKA_TOPIC  = os.getenv("KAFKA_TOPIC",  "datacenter.metrics.event")
INTERVAL     = float(os.getenv("INTERVAL", "1.0"))

# Iceberg catalog (Nessie REST)
CATALOG_URI = os.getenv("CATALOG_URI", "http://nessie.observability.svc.cluster.local:19120/nessie_db/")
ENDPOINT    = os.getenv("ENDPOINT",    "http://minio.minio.svc.cluster.local:9000")
ACCESSKEY   = os.getenv("AWS_ACCESS_KEY_ID",     "rook-ceph")
SECRETKEY   = os.getenv("AWS_SECRET_ACCESS_KEY", "rook-ceph")
WAREHOUSE   = os.getenv("WAREHOUSE", "s3://telemetry/")

# Nessie auth
NESSIE_USER     = os.getenv("NESSIE_USER",     "nessie_lover")
NESSIE_PASSWORD = os.getenv("NESSIE_PASSWORD", "nessie_love")

# Table
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "infra")
ICEBERG_TABLE     = os.getenv("ICEBERG_TABLE",     "node_events")
