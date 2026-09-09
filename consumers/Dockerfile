FROM python:3.11-slim

WORKDIR /app

COPY consumers/requirements.txt /app/consumers/requirements.txt
RUN pip install --no-cache-dir -r /app/consumers/requirements.txt

COPY consumers /app/consumers
COPY shared /app/shared

ENTRYPOINT ["python", "consumers/consumer.py"]
CMD ["--consumer-id", "consumer-01"]