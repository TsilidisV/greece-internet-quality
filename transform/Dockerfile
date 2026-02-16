# 1. Use a slim Python base image (Debian-based for Java compatibility)
FROM python:3.11-slim-bookworm

# 2. Prevent Python from buffering stdout (so logs show up immediately in Cloud Logging)
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    # Point to the location where uv installs packages
    VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:$PATH" \
    # Spark Specific Envs
    JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64" \
    SPARK_HOME="/app/.venv/lib/python3.11/site-packages/pyspark"

# 3. Install System Dependencies (Java 17 is required for newer Spark)
# We use --no-install-recommends to keep the image small
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 4. Install uv (The magic tool)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 5. Set working directory
WORKDIR /app

# 6. Copy dependency definitions
COPY pyproject.toml uv.lock ./ 
# (Or requirements.txt if you prefer, but pyproject.toml is modern)

# 7. Install dependencies using uv
# --frozen: strictly use the lockfile (secure)
# --no-cache: keep image small
RUN uv sync --frozen --no-cache

# 8. Copy your transformation code
COPY src/ .

# 9. Run the ETL job
CMD ["python", "check_connections.py"]