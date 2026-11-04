FROM docker.io/alpine:3.24

RUN apk add --no-cache curl jq

COPY init.sh /init.sh

ENTRYPOINT ["/bin/sh", "/init.sh"]
