FROM golang:1.27.1-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /dataslate-api ./cmd/dataslate-api

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /dataslate-api /dataslate-api
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/dataslate-api"]
CMD ["serve"]
