# ---- build stage ----
FROM golang:1.22-alpine AS build

# 国内 Go 模块代理加速
ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/crawler ./cmd/crawler

# ---- runtime stage ----
FROM alpine:3.20
WORKDIR /app
RUN apk add --no-cache ca-certificates
COPY --from=build /out/crawler /app/crawler
COPY config.docker.yaml /app/config.docker.yaml
ENTRYPOINT ["/app/crawler", "/app/config.docker.yaml"]
