FROM python:3.8.6

ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH='.'

COPY ./requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

ENV APP_DIR /usr/src/app
WORKDIR $APP_DIR
COPY . .

