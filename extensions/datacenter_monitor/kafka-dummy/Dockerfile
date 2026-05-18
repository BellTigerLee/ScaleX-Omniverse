FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir confluent-kafka

COPY dummy_producer.py .

# 환경변수로 설정값 전달 (docker-compose.yaml의 environment 섹션 참조)
CMD ["sh", "-c", "python dummy_producer.py --host ${KAFKA_BROKER} --topic ${KAFKA_TOPIC} --interval ${INTERVAL}"]
