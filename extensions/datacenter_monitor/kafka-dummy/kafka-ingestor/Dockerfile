FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyiceberg[pyarrow,s3]>=0.7.0" \
    pyarrow \
    aiokafka

COPY ingestor.py .

CMD ["python", "ingestor.py"]
