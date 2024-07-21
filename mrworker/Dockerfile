# syntax=docker/dockerfile:1

FROM golang:1.18-alpine

RUN apk add build-base

WORKDIR /app
COPY . ./
RUN go mod download

WORKDIR /app/main
RUN go build -buildmode=plugin ../mrapps/wc.go
RUN go build -buildmode=plugin ../mrapps/rtiming.go
RUN go build -buildmode=plugin ../mrapps/nocrash.go
RUN go build -buildmode=plugin ../mrapps/mtiming.go
RUN go build -buildmode=plugin ../mrapps/jobcount.go
RUN go build -buildmode=plugin ../mrapps/indexer.go
RUN go build -buildmode=plugin ../mrapps/early_exit.go
RUN go build -buildmode=plugin ../mrapps/crash.go
RUN go build mrworker.go

EXPOSE 8002
EXPOSE 8003

ENTRYPOINT [ "/app/main/mrworker" ]
