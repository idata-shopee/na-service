FROM alpine:3.7
RUN apk add --no-cache ca-certificates openssl
WORKDIR /opt
ADD --chown=daemon:daemon bin /opt
USER daemon
ENTRYPOINT ["bin/na_service"]
CMD []
