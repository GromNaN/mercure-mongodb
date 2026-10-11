MONGODB_URI ?= mongodb://localhost:27017/?directConnection=true
HUB ?= http://localhost:3000/.well-known/mercure
CLIENTS ?= 1000
MESSAGES ?= 1000

# The image targets Linux. Build the hub binary for the host architecture, so
# the local image matches the machine. GoReleaser builds both amd64 and arm64
# for a release.
GOOS ?= linux
GOARCH ?= $(shell go env GOARCH)
TARGETPLATFORM ?= $(GOOS)/$(GOARCH)

# Build tags for the hub binary and the container image, the same as the Mercure
# hub release. The deprecated_* tags compile in the Mercure v0.x compatibility
# mode, which only takes effect when a configuration sets
# protocol_version_compatibility. The nobadger, nomysql and nopgx tags match the
# upstream release build; Mercure v1.1.0 no longer reads them.
HUB_TAGS ?= deprecated_transport,deprecated_topic,deprecated_claim,nobadger,nomysql,nopgx

.PHONY: build build-hub docker licenses test test-integration load fmt vet tidy

build:
	go build ./...

# Build the hub binary locally, with the Mercure handler and this transport.
build-hub:
	mkdir -p $(TARGETPLATFORM)
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -tags '$(HUB_TAGS)' -o $(TARGETPLATFORM)/mercure-mongodb ./caddy/mercure

# Collect the license and notice files of every module the hub binary links.
# The tree is generated, not committed, and both the image and the release
# archives copy it in.
licenses:
	./.github/scripts/collect-licenses.sh

# Build the container image. The Dockerfile expects the hub binary under the
# target platform directory, the way GoReleaser lays it out.
docker: build-hub licenses
	docker build --build-arg TARGETPLATFORM=$(TARGETPLATFORM) -t mercure-mongodb .

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
