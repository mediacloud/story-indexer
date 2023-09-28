# pull official base image
FROM python:3.10-slim-bookworm as builder

# set work directory
WORKDIR /app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && \
    if [ "$(uname -m)" = "aarch64" ]; then \
        apt-get install -y gcc; \
    fi

# install dependencies
COPY ../requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt


FROM python:3.10-slim-bookworm

ENV APP_HOST=.
ENV APP_DOCKER=/app

WORKDIR ${APP_DOCKER}

COPY --from=builder /usr/src/app/wheels /wheels
COPY --from=builder /app/requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache /wheels/*


COPY ../bin/ ${APP_DOCKER}/bin/
RUN chmod +x ${APP_DOCKER}/bin/*


COPY ../indexer ${APP_DOCKER}/indexer/
# pass the executable as ARG and use it in CMD
ARG RUN
ENV RUN=${RUN}

ENTRYPOINT [ "/bin/sh", "-c", "${APP_DOCKER}/bin/indirect.sh" ]
