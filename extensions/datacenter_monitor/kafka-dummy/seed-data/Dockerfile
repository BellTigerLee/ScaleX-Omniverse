FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "pyiceberg[pyarrow,s3fs]>=0.7.0" \
    pyarrow \
    s3fs \
    boto3

COPY seed.py .

CMD ["python", "seed.py"]
