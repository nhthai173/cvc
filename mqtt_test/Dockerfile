FROM python:3.11-slim

#
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/app

#
COPY mqtt-test/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#
COPY mqtt-test ./mqtt-test
COPY common ./common

#
RUN pip install --no-cache-dir -e ./common

# Non-root runtime
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /usr/app
USER appuser

# Env
ENV PYTHONUNBUFFERED=1

#
CMD ["python", "-u", "mqtt-test/mqtt_subscriber.py"]
