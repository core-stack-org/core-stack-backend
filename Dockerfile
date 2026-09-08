# Build with docker-compose.maintain.yml. Users pull this image and run docker-compose.yml.
FROM mambaorg/micromamba:1.5.10-bookworm

ARG MAMBA_DOCKERFILE_ACTIVATE=1
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    BACKEND_DIR=/app \
    DATA_DIR=/var/tmp/core-stack-data \
    MAMBA_DOCKERFILE_ACTIVATE=1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gdal-bin \
        libpq5 \
        p7zip-full \
        procps \
        unzip \
        wget \
    && rm -rf /var/lib/apt/lists/*

COPY installation/environment.yml /tmp/environment.yml
RUN micromamba create -y -n corestackenv -f /tmp/environment.yml \
    && micromamba clean --all --yes \
    && rm -f /tmp/environment.yml

ENV PATH="/opt/conda/envs/corestackenv/bin:${PATH}" \
    MAMBA_DEFAULT_ENV=corestackenv

WORKDIR /app
COPY . /app
COPY installation/docker/entrypoint.sh \
     installation/docker/geoserver-init.sh \
     installation/docker/gee-config.sh \
     installation/docker/download-data.sh \
     /usr/local/bin/
RUN micromamba run -n corestackenv python -m pip install --no-cache-dir gdown \
    && chmod +x /usr/local/bin/entrypoint.sh \
        /usr/local/bin/geoserver-init.sh \
        /usr/local/bin/gee-config.sh \
        /usr/local/bin/download-data.sh \
    && mkdir -p /app/data/gee_confs /var/tmp/core-stack-data

EXPOSE 8000
ENTRYPOINT ["/usr/local/bin/_entrypoint.sh"]
CMD ["/usr/local/bin/entrypoint.sh"]
