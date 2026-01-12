# Use Python 3.10 slim-bookworm image (stable Debian 12 base)
FROM python:3.10-slim-bookworm

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies
# - OpenJDK 17: Required for PySpark
# - curl & ca-certificates: Required to install uv
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv using the standalone installer (per official docs)
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure uv is on the PATH
ENV PATH="/root/.local/bin/:$PATH"
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies using uv
# --system: Install into the system Python environment (avoids creating a venv inside Docker)
RUN uv pip install --system --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Create directories for data and logs if they don't exist
# This ensures permissions are correct when we mount volumes
RUN mkdir -p data/raw data/processed logs output/visualizations

# Default command to run the pipeline
CMD ["python", "main.py"]
