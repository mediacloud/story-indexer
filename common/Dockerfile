FROM python:3.9.12-slim-bullseye

ARG RABBITMQ_URL

WORKDIR /app

ENV PYTHONPATH="${PYTHONPATH}:/app/common"
ENV RABBITMQ_URL=${RABBITMQ_URL}

COPY requirements-worker.txt .

RUN pip install --upgrade pip && pip install -r requirements-worker.txt

COPY . .
