FROM python:3.10-slim

COPY . /fiss

RUN set -ex \
    && apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -U --upgrade-strategy eager /fiss \
    && rm -rf /fiss

CMD ["fissfc"]
