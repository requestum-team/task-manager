# pull official base image
FROM python:3.11.3-alpine as app

# set work directory
WORKDIR /usr/src/app

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install psycopg2
RUN apk update \
    && apk add --virtual build-deps gcc python3-dev musl-dev \
    && apk add postgresql postgresql-contrib postgresql-dev \
    && pip install psycopg2 \
    && pip install -U pip setuptools wheel \
    && apk del build-deps

RUN apk update && apk add python3-dev \
                        gcc \
                        libc-dev \
                        libffi-dev

# copy project
COPY . /usr/src/app/

# install dependencies
RUN pip install --upgrade pip
RUN pip install pipenv
RUN pipenv install


EXPOSE $APP_PORT
