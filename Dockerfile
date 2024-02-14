FROM ubuntu:20.04
RUN apt-get -y update && \
    apt-get install -y build-essential && \
    apt-get -y install curl

WORKDIR /app
COPY ./bin/broker ./broker
COPY ./config/config.yml ./config/config.yml
EXPOSE 8080

ENTRYPOINT ["/app/broker"]
