# Dockerfile
FROM python:3.12-alpine3.22

COPY --from=ghcr.io/astral-sh/uv:0.10.9 /uv /uvx /bin/

WORKDIR /app

COPY requirements.txt .
COPY pyproject.toml uv.lock ./
RUN apk update
RUN apk add postgresql-dev gcc python3-dev musl-dev
RUN uv sync --frozen --no-dev

COPY src/ src/
COPY seed/ seed/
COPY main.py .

CMD ["uv", "run", "python", "main.py"]
