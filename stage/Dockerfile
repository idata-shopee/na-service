FROM alpine:3.7
RUN apk add --no-cache ca-certificates openssl
WORKDIR /
ADD --chown=daemon:daemon bin /bin
USER daemon
ENTRYPOINT ["bin/service"]
CMD []
