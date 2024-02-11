FROM ubuntu:20.04
RUN apt-get update && \
    apt-get install -y build-essential

WORKDIR /app
COPY ./bin/broker ./broker
COPY ./config/config.yml ./config/config.yml
EXPOSE 8080

ENTRYPOINT ["/app/broker"]
