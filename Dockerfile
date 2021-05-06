FROM alpine:3.13

RUN apk add --no-cache ca-certificates \
 && adduser -D -u 1000 jx

COPY ./build/linux/lighthouse-telemetry-plugin /app/

WORKDIR /app
USER 1000

ENTRYPOINT ["/app/lighthouse-telemetry-plugin"]