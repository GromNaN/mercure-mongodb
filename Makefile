MONGODB_URI ?= mongodb://localhost:27017/?directConnection=true
HUB ?= http://localhost:3000/hub
CLIENTS ?= 1000
MESSAGES ?= 1000

.PHONY: build test test-integration load fmt vet tidy

build:
	go build ./...

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
