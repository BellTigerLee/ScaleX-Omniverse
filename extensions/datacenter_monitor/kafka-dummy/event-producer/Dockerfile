FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyiceberg[pyarrow,s3fs]>=0.7.0" \
    pyarrow \
    s3fs \
    boto3 \
    confluent-kafka

COPY event_producer.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "event_producer.py"]
