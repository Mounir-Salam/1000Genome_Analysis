# Use Python 3.12 as the main "Brain" environment
FROM python:3.12-slim-bookworm

# 1. Install System Dependencies (Java for Spark, Procps for Dagster)
RUN apt-get update && apt-get install -y \
    default-jre-headless \
    procps \
    curl \
    && apt-get clean

# 2. Install 'uv' for fast environment management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_HTTP_TIMEOUT=600

# 3. Set the working directory
WORKDIR /app

# 4. Copy and install main python environment (Python 3.12)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# 5. Copy the rest of the project and configure uv to treat it as a project
COPY . .
RUN uv sync --frozen --no-dev

# 6. Create isolated dbt venv (Python 3.11)
RUN uv venv dbt_project/dbt_venv --python 3.11
RUN uv pip install -r dbt_requirements.txt --python dbt_project/dbt_venv/bin/python

# 7. Expose Dagster UI port
EXPOSE 3000

# 8. Set Environment Variables (Defaults)
ENV DAGSTER_HOME=/app/.dagster
RUN mkdir -p .dagster

# 9. Launch Dagster
CMD ["/app/.venv/bin/dagster", "dev", "-h", "0.0.0.0", "-m", "orchestration"]