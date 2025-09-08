FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=src/consumer/uv.lock,target=uv.lock \
    --mount=type=bind,source=src/consumer/pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

ADD src/consumer/ /app
COPY src/shared/schemas.py /app/
COPY src/shared/raytracer.py /app/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT []

CMD ["uv", "run", "--frozen", "main.py"]
