FROM python:3.12-alpine3.22

COPY --from=ghcr.io/astral-sh/uv:0.10.5 /uv /uvx /bin/

WORKDIR /app

COPY requirements.txt .
COPY pyproject.toml uv.lock ./

# Install system dependencies required for building Python packages
RUN apk update && apk add --no-cache \
    postgresql-dev \
    gcc \
    python3-dev \
    musl-dev \
    libffi-dev \
    openssl-dev \
    libstdc++

# Sync dependencies
RUN uv sync --frozen --no-dev

COPY . .

CMD ["uv", "run", "python", "main.py"]
