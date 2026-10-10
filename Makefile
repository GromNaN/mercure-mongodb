MONGODB_URI ?= mongodb://localhost:27017/?directConnection=true
HUB ?= http://localhost:3000/.well-known/mercure
CLIENTS ?= 1000
MESSAGES ?= 1000

# Build tags for the hub binary and the container image, the same as the Mercure
# hub release, so the image accepts the deprecated options.
HUB_TAGS ?= deprecated_transport,deprecated_topic,deprecated_claim,nobadger,nomysql,nopgx

.PHONY: build build-hub docker test test-integration load fmt vet tidy

build:
	go build ./...

# Build the hub binary locally, with the Mercure handler and this transport.
build-hub:
	mkdir -p linux/amd64
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags '$(HUB_TAGS)' -o linux/amd64/mercure-mongodb ./caddy/mercure

# Build the container image. The Dockerfile expects the hub binary under the
# target platform directory, the way GoReleaser lays it out.
docker: build-hub
	docker build --build-arg TARGETPLATFORM=linux/amd64 -t mercure-mongodb .

# Unit tests. They run without MongoDB.
test:
	go test -race ./...

# Integration tests. They need a replica set, for example the one started by
# `docker compose up -d`.
test-integration:
	MONGODB_URI='$(MONGODB_URI)' go test -race -run TestIntegration -count=1 ./...

# SSE load generator. It needs a running hub and two JWTs.
load:
	go run ./load -hub $(HUB) \
		-publish-token '$(PUBLISHER_JWT)' \
		-subscribe-token '$(SUBSCRIBER_JWT)' \
		-clients $(CLIENTS) -messages $(MESSAGES)

fmt:
	gofmt -w .

vet:
	go vet ./...

tidy:
	go mod tidy
