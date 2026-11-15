FROM docker.io/alpine:3.24

RUN apk add --no-cache curl jq

COPY init.sh /init.sh

# The repository may be checked out on Windows with CRLF line endings; strip
# them so /bin/sh can parse the script inside the Linux container.
RUN sed -i 's/\r$//' /init.sh

ENTRYPOINT ["/bin/sh", "/init.sh"]
