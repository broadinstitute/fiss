FROM python:2.7.11

RUN apt-get update && apt-get install -y curl
RUN pip install fissfc
RUN curl https://sdk.cloud.google.com | bash
