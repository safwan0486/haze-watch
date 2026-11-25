FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HAZEWATCH_HOME=/app

# Install dependencies first so this layer is cached between code changes.
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir ".[dashboard]"

COPY config ./config
COPY sql ./sql
COPY dashboard ./dashboard

RUN useradd --create-home --uid 1000 hazewatch && mkdir -p /app/data && chown hazewatch /app/data
USER hazewatch

# Default: one full pipeline run. The dashboard service overrides this command.
CMD ["hazewatch", "run", "--past-days", "7"]
