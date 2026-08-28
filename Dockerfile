# syntax=docker/dockerfile:1
# modernc.org/sqlite (CGO-free) requires Go >= 1.25.
FROM golang:1.27-alpine AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/kafka-phoenix-ext ./cmd/kafka-phoenix-ext

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/kafka-phoenix-ext /kafka-phoenix-ext
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/kafka-phoenix-ext"]