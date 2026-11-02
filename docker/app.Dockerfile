# syntax=docker/dockerfile:1
# The one Python image of the stack: workload generator, dbt runner (dbt installed from the `dbt`
# extra, run only as a subprocess) and NiFi token refresher later. Built once (compose service
# `dbt-runner`), reused by tag elsewhere.

FROM ghcr.io/astral-sh/uv:0.13.6@sha256:af22a52f5eafef3d78e551599d405c02df61919d18028f0cea8eda93bc9986d0 AS uv

FROM python:3.12.15-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70 AS build
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app
# Dependencies first: this layer only changes when pyproject.toml or uv.lock do.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    uv sync --locked --no-dev --no-editable --extra dbt --no-install-project
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable --extra dbt

# Same base as the builder, so the venv's interpreter path and ABI match.
FROM python:3.12.15-slim-bookworm@sha256:6a0e06f5ba0b373d051aabd3fffb9b873f27f9a2a3a14bcbe37dee6f0e3bbc70 AS app
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app
COPY --from=build /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1
# The dbt project, read-only at run time. The runner's
# state, dbt targets and logs live on a volume that inherits this directory's owner.
COPY --chown=app:app dbt /home/app/dbt
RUN mkdir /var/lib/dbt-runner && chown app:app /var/lib/dbt-runner
WORKDIR /home/app
USER app
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["retail-pricing", "version"]
ENTRYPOINT ["retail-pricing"]
