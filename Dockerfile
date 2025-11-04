# ---------- builder ----------
FROM python:3.9-slim-bookworm AS builder

WORKDIR /app

# System build deps (trimmed + cleaned)
RUN apt-get update -qq \
 && apt-get install --no-install-recommends -y -qq \
      curl ca-certificates g++ gcc git openmpi-bin libopenmpi-dev \
      libgdal-dev libboost-iostreams-dev python3-dev \
 && rm -rf /var/lib/apt/lists/*

# Install uv
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh > /dev/null && rm /uv-installer.sh
ENV PATH="/root/.local/bin:$PATH"

# Create a dedicated venv
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# App requirements (optional user reqs)
COPY requirements.txt /opt/necessary/requirements.txt

# ---- Core Python deps (install into THIS venv explicitly) ----
# Spark Connect client needs these:
RUN uv pip install --python /opt/venv/bin/python -q \
      "grpcio>=1.48.1" \
      "grpcio-status>=1.48.1" \
      "googleapis-common-protos>=1.56.0" \
      "pyarrow>=14.0.0"

# Any extra libs you need before final (example: richdem)
RUN uv pip install --python /opt/venv/bin/python -q richdem

# Your project’s requirements
RUN uv pip install --python /opt/venv/bin/python --strict --quiet --no-cache -r /opt/necessary/requirements.txt \
 && uv cache clean && uv cache prune


# ---------- final ----------
FROM python:3.9-slim-bookworm AS final

WORKDIR /app

# Minimal runtime OS deps (Debian; no Ubuntu PPA on slim-bookworm)
RUN apt-get update -qq \
 && apt-get install --no-install-recommends -y -qq \
      wget curl ca-certificates \
      gdal-bin libgdal-dev \
      gcc build-essential libtiff5-dev \
 && rm -rf /var/lib/apt/lists/*

# Bring over the venv and uv CLI
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /root/.local/bin /root/.local/bin

ENV PATH="/root/.local/bin:$PATH"
ENV VIRTUAL_ENV="/opt/venv"
ENV PATH="/opt/venv/bin:$PATH"

# Install GDAL Python wheel matching system GDAL (into the SAME venv)
# (libgdal-dev provides gdal-config)
RUN uv pip install --python /opt/venv/bin/python -q GDAL==$(gdal-config --version)

# ----- Java + Spark -----
# Java 17
RUN mkdir -p /opt/spark_home \
 && wget -q https://builds.openlogic.com/downloadJDK/openlogic-openjdk/17.0.16+8/openlogic-openjdk-17.0.16+8-linux-x64.tar.gz \
 && tar -xf openlogic-openjdk-17.0.16+8-linux-x64.tar.gz -C /opt/spark_home \
 && rm -f openlogic-openjdk-17.0.16+8-linux-x64.tar.gz
ENV JAVA_HOME="/opt/spark_home/openlogic-openjdk-17.0.16+8-linux-x64"
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Spark 4.0.1
RUN wget -q https://archive.apache.org/dist/spark/spark-4.0.1/spark-4.0.1-bin-hadoop3.tgz \
 && tar -xf spark-4.0.1-bin-hadoop3.tgz -C /opt/spark_home \
 && rm -f spark-4.0.1-bin-hadoop3.tgz
ENV SPARK_HOME="/opt/spark_home/spark-4.0.1-bin-hadoop3"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/:${PYTHONPATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.9-src.zip:${PYTHONPATH}"

# Install PySpark with Connect extras into the venv (pulls client deps)
RUN uv pip install --python /opt/venv/bin/python -q "pyspark[connect]==4.0.1"

# (Optional) your entrypoint / scripts
COPY entrypoint.sh /opt/necessary/entrypoint.sh
RUN chmod u+x /opt/necessary/entrypoint.sh

EXPOSE 8888

CMD ["/opt/necessary/entrypoint.sh"]




