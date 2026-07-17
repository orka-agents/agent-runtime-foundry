# syntax=docker/dockerfile:1
FROM golang:1.26.5-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN mkdir -p /out && CGO_ENABLED=0 GOOS=linux go build -o /out/agent-runtime-foundry .

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/agent-runtime-foundry /agent-runtime-foundry
USER 65532:65532
EXPOSE 8090
ENTRYPOINT ["/agent-runtime-foundry"]
