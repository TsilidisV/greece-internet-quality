# Use a lightweight Python base image
FROM python:3.11-slim

# 1. Copy uv directly from the official image (Best Practice)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 2. Install Java (Required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 3. Set environment variables for uv
ENV UV_SYSTEM_PYTHON=1
# Make sure uv doesn't hang waiting for user input
ENV UV_NO_CACHE=1 

WORKDIR /app

# 4. Copy dependency files
COPY pyproject.toml uv.lock ./

# 5. Install dependencies
# (Since uv is in /bin/, this will definitely be found)
RUN uv sync --frozen

# 8. Copy your transformation code
COPY src/ .

# 9. Run the ETL job
CMD ["uv", "run", "medallion.py"]