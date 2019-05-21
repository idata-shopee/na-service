FROM alpine:3.7
RUN apk add --no-cache ca-certificates openssl
WORKDIR /opt/docker
ADD --chown=daemon:daemon opt /opt
USER daemon
ENTRYPOINT ["bin/na_service"]
CMD []
