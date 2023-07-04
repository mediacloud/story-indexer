# pull official base image
FROM python:3.10-slim-bullseye as builder

# set work directory
WORKDIR /app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update

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

COPY ../bin/func.sh ${APP_DOCKER}/bin/
COPY ../bin/indirect.sh ${APP_DOCKER}/bin/
COPY ../bin/run-extract.sh ${APP_DOCKER}/bin


RUN chmod +x ${APP_DOCKER}/bin/indirect.sh


COPY ../indexer ${APP_DOCKER}/indexer/
# pass the executable as ARG and use it in CMD
ARG RUN
ENV RUN=${RUN}

ENTRYPOINT [ "/bin/sh", "-c", "${APP_DOCKER}/bin/indirect.sh" ]
