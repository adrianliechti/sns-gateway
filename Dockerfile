FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -trimpath -o /gateway ./cmd/gateway

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 gateway
COPY --from=build /gateway /usr/local/bin/gateway
USER gateway
EXPOSE 9001
ENTRYPOINT ["gateway"]
CMD ["-listen", "0.0.0.0:9001"]
