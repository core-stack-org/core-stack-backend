# ============================================================
# CoRE Stack Backend — Base Image
# Base: Ubuntu 24.04 LTS (Noble Numbat)
#
# Provides the OS-level dependencies that install.sh cannot
# easily handle itself:
#   - PostgreSQL
#   - Erlang 27 (compiled from source — Ubuntu 24.04 ships
#     Erlang 24 which is too old for RabbitMQ 4.x)
#   - RabbitMQ 4.3.1
#   - Miniconda + corestack-backend conda environment
#
# After running the container, complete setup with:
#   bash installation/install.sh \
#     --skip unzip_install,miniconda,rabbitmq,conda_env,geoserver
# ============================================================

FROM --platform=linux/arm64 ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# ── 1. Base system tools ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    git wget curl build-essential libpq-dev unzip \
    sudo ca-certificates gnupg lsb-release \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── 2. PostgreSQL ─────────────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    postgresql postgresql-contrib \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── 3. Erlang build dependencies ─────────────────────────────────────────────
RUN apt-get update && apt-get install -y \
    autoconf m4 libssl-dev libncurses-dev socat logrotate \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# ── 4. Erlang OTP 27.3.4.8 — compiled from source ────────────────────────────
# Ubuntu 24.04 ships Erlang 24 which is too old for RabbitMQ 4.x.
ARG OTP_VERSION=27.3.4.8
RUN curl -fsSL \
    https://github.com/erlang/otp/releases/download/OTP-${OTP_VERSION}/otp_src_${OTP_VERSION}.tar.gz \
    -o /tmp/otp_src.tar.gz

RUN tar -zxf /tmp/otp_src.tar.gz -C /tmp && rm /tmp/otp_src.tar.gz

WORKDIR /tmp/otp_src_${OTP_VERSION}

RUN export ERL_TOP=$(pwd) && ./otp_build autoconf

RUN export ERL_TOP=$(pwd) && ./configure \
    --without-javac \
    --without-wx \
    --without-odbc

RUN make -j$(nproc)

RUN make install && rm -rf /tmp/otp_src_${OTP_VERSION}

# ── 5. RabbitMQ 4.3.1 ────────────────────────────────────────────────────────
WORKDIR /

RUN curl -fsSL \
    https://github.com/rabbitmq/rabbitmq-server/releases/download/v4.3.1/rabbitmq-server_4.3.1-1_all.deb \
    -o /tmp/rabbitmq.deb \
    && dpkg -i --force-depends /tmp/rabbitmq.deb \
    && rm /tmp/rabbitmq.deb

# Tell RabbitMQ where the source-built Erlang lives
RUN mkdir -p /etc/rabbitmq \
    && echo 'ERLANG_HOME=/usr/local' > /etc/rabbitmq/rabbitmq-env.conf \
    && echo 'PATH=/usr/local/bin:/usr/bin:/bin' >> /etc/rabbitmq/rabbitmq-env.conf \
    && echo 'deprecated_features.permit.transient_nonexcl_queues = true' > /etc/rabbitmq/rabbitmq.conf

# ── 6. Miniconda ──────────────────────────────────────────────────────────────
ENV CONDA_DIR=/opt/conda
ENV PATH="${CONDA_DIR}/bin:${PATH}"

RUN ARCH=$(uname -m) \
    && wget -q \
        "https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-${ARCH}.sh" \
        -O /tmp/miniconda.sh \
    && bash /tmp/miniconda.sh -b -p ${CONDA_DIR} \
    && rm /tmp/miniconda.sh

RUN conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main || true \
    && conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r || true \
    && conda config --set channel_priority flexible \
    && conda clean -afy

# ── 7. Clone repo and create conda environment ────────────────────────────────
WORKDIR /opt
RUN git clone https://github.com/core-stack-org/core-stack-backend.git corestack
WORKDIR /opt/corestack

RUN conda env create -f installation/environment.yml \
    && conda clean -afy

# ── 8. Fix missing/broken packages ───────────────────────────────────────────
SHELL ["/opt/conda/bin/conda", "run", "-n", "corestack-backend", "/bin/bash", "-c"]

RUN pip install pyogrio "setuptools<81" psycopg2-binary

# ── 9. Create required runtime data directories ───────────────────────────────
RUN mkdir -p \
    /opt/corestack/data/fc_to_shape \
    /opt/corestack/data/admin-boundary/input \
    /opt/corestack/data/admin-boundary/output \
    /opt/corestack/data/excel_files \
    /opt/corestack/data/tmp \
    /opt/corestack/bot_interface/whatsapp_media \
    /opt/corestack/data/activated_locations && \
    echo '[]' > /opt/corestack/data/activated_locations/active_locations.json
RUN chmod -R 755 /opt/conda/envs/corestack-backend/share/proj

# ── 10. Patch api.py — guard os.makedirs against empty WHATSAPP_MEDIA_PATH ───
RUN sed -i \
    's|os.makedirs(WHATSAPP_MEDIA_PATH, exist_ok=True)|if WHATSAPP_MEDIA_PATH: os.makedirs(WHATSAPP_MEDIA_PATH, exist_ok=True)|' \
    /opt/corestack/bot_interface/api.py

# Disable automatic GeoServer style assignment — styles can be applied
# manually via the GeoServer UI. The auto-assignment fails if the named
# style doesn't exist, causing a 500 error that aborts the whole task.
RUN sed -i \
    's|    if style_name:|    if False:  # style disabled — apply via GeoServer UI|' \
    /opt/corestack/utilities/gee_utils.py

# ── 11. Patch computing/tasks.py — register all Celery tasks ─────────────────
RUN cat > /opt/corestack/computing/tasks.py << 'TASKS'
from computing.STAC_specs.stac_collection import generate_stac_collection_task
import computing.change_detection.change_detection
import computing.change_detection.change_detection_vector
import computing.clart.clart
import computing.clart.drainage_density
import computing.clart.fes_clart_to_geoserver
import computing.clart.lithology
import computing.crop_grid.crop_grid
import computing.cropping_intensity.cropping_intensity
import computing.drought.drought
import computing.drought.drought_causality
import computing.layer_dependency.layer_generation_in_order
import computing.lulc.lulc_v3
import computing.lulc.lulc_vector
import computing.lulc.river_basin_lulc.lulc_v2_river_basin
import computing.lulc.river_basin_lulc.lulc_v3_river_basin_using_v2
import computing.lulc.tehsil_level.lulc_v2
import computing.lulc.tehsil_level.lulc_v3
import computing.lulc.v4.lulc_v4
import computing.lulc_X_terrain.lulc_on_plain_cluster
import computing.lulc_X_terrain.lulc_on_slope_cluster
import computing.misc.admin_boundary
import computing.misc.agroecological_space
import computing.misc.antyodaya
import computing.misc.aquifer_vector
import computing.misc.canal_layer
import computing.misc.catchment_area
import computing.misc.digital_elevation_model
import computing.misc.distancetonearestdrainage
import computing.misc.drainage_lines
import computing.misc.facilities_proximity
import computing.misc.factory_csr
import computing.misc.green_credit
import computing.misc.lcw_conflict
import computing.misc.mining_data
import computing.misc.naturaldepression
import computing.misc.ndvi_time_series
import computing.misc.nrega
import computing.misc.restoration_opportunity
import computing.misc.slope_percentage
import computing.misc.soge_vector
import computing.misc.stream_order
import computing.mws.generate_hydrology
import computing.mws.mws
import computing.mws.mws_centroid
import computing.mws.mws_connectivity
import computing.plantation.site_suitability
import computing.surface_water_bodies.merge_swb_ponds
import computing.surface_water_bodies.swb
import computing.terrain_descriptor.terrain_clusters
import computing.terrain_descriptor.terrain_raster
import computing.terrain_descriptor.terrain_raster_fabdem
import computing.tree_health.canopy_height
import computing.tree_health.canopy_height_vector
import computing.tree_health.ccd
import computing.tree_health.ccd_vector
import computing.tree_health.overall_change
import computing.tree_health.overall_change_vector
import computing.zoi_layers.zoi
__all__ = ['generate_stac_collection_task']
TASKS

# ── 12. Copy patched install.sh ───────────────────────────────────────────────
# Copies the patched install-docker.sh from installation/ in the repo root.
# Place install-docker.sh at installation/install-docker.sh in your local clone.
# This is a patched version of install.sh with Docker-specific fixes —
# the original install.sh in the repo is left untouched.
ARG INSTALL_CACHE_BUST=1
COPY installation/install-docker.sh /opt/corestack/installation/install-docker.sh
RUN chmod +x /opt/corestack/installation/install-docker.sh

# ── 13. Startup script ────────────────────────────────────────────────────────
RUN cat > /start.sh << 'STARTEOF'
#!/bin/bash
service postgresql start
service rabbitmq-server start

until su -c "pg_isready -q" postgres; do
    sleep 1
done

# Create DB user and database matching install.sh defaults
su -c "psql -c \"CREATE USER corestack_admin WITH PASSWORD 'corestack@123' SUPERUSER;\" " postgres 2>/dev/null || true
su -c "psql -c \"CREATE DATABASE corestack_db OWNER corestack_admin;\" " postgres 2>/dev/null || true

echo ""
echo "============================================================"
echo "  CoRE Stack container ready."
echo ""
echo "  Complete setup by running:"
echo "    bash installation/install-docker.sh \\"
echo "      --skip unzip_install,miniconda,rabbitmq,conda_env,geoserver"
echo "============================================================"
echo ""
exec "$@"
STARTEOF
RUN chmod +x /start.sh

# ── 14. Environment ───────────────────────────────────────────────────────────
ENV PROJ_DATA=/opt/conda/envs/corestack-backend/share/proj
ENV PROJ_LIB=/opt/conda/envs/corestack-backend/share/proj

EXPOSE 8000
WORKDIR /opt/corestack
ENTRYPOINT ["/start.sh"]
CMD ["bash"]
