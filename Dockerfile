# Use official Spark image with Python 3 and Java 17
FROM spark:python3-java17

# Switch to root to install packages
USER root

# 1. Install uv for fast package management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 2. Copy requirements
COPY requirements.txt /tmp/

# 3. Install dependencies with uv (fast!)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r /tmp/requirements.txt

# 4. Install Jupyter for notebook support
RUN uv pip install --system jupyterlab ipykernel

# 5. Create home directory for spark user (the image uses /nonexistent by default)
# Also create Jupyter runtime directories to avoid permission errors
RUN mkdir -p /home/spark/work /home/spark/src /home/spark/data \
             /home/spark/.local/share/jupyter/runtime \
             /home/spark/.jupyter && \
    chown -R spark:spark /home/spark && \
    usermod -d /home/spark spark

# Switch to spark user
USER spark

# Set the working directory
WORKDIR /home/spark/work

# Environment variables
ENV HOME=/home/spark
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV JUPYTER_RUNTIME_DIR=/home/spark/.local/share/jupyter/runtime