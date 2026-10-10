#!/usr/bin/env sh
#
# Smoke test a mercure-mongodb container image.
#
# Starts the hub, waits for it to serve its public endpoint, then checks a full
# publish/subscribe round trip: a subscriber opens an SSE stream and the update
# published on the same topic must reach it through MongoDB.
#
# It targets a Linux host: the container runs with host networking, so the hub
# reaches the MongoDB replica set on localhost and the test reaches the hub on
# localhost:3000. This is how the CI docker job runs it.
#
# Environment:
#   MONGODB_URI  MongoDB connection string, on a replica set.
#                Defaults to mongodb://localhost:27017/?directConnection=true.
#
# Usage: .github/scripts/smoke-test-image.sh [image]
#
set -eu

image="${1:-mercure-mongodb:ci}"
mongodb_uri="${MONGODB_URI:-mongodb://localhost:27017/?directConnection=true}"

container=hub
port=3000
issuer="http://localhost:${port}"
audience="${issuer}/.well-known/mercure"
key=change-me
topic='https://example.com/books/1'

sse_out=$(mktemp)
sse_headers=$(mktemp)
subscriber=

cleanup() {
	status=$?
	# Kill the SSE subscriber first, so no curl keeps running after an early
	# exit and no stream is left open against the container that is about to go.
	if [ -n "$subscriber" ]; then
		kill "$subscriber" 2>/dev/null || true
		wait "$subscriber" 2>/dev/null || true
	fi
	# Dump the hub logs only on failure: they explain what went wrong.
	if [ "$status" -ne 0 ]; then
		docker logs "$container" >&2 || true
	fi
	docker rm -f "$container" >/dev/null 2>&1 || true
	rm -f "$sse_out" "$sse_headers"
}
trap cleanup EXIT

echo "starting the hub from ${image}"
docker run -d --name "$container" --network host \
	-e SERVER_NAME=":${port}" \
	-e MERCURE_TRUSTED_ISSUERS="$issuer" \
	-e MERCURE_MONGODB_URI="$mongodb_uri" \
	-e MERCURE_PUBLISHER_JWT_KEY="$key" \
	-e MERCURE_SUBSCRIBER_JWT_KEY="$key" \
	"$image"

# The admin health endpoint reports ready while Caddy is still provisioning the
# mercure handler, before the public listener accepts connections. Wait for the
# health endpoint, then wait for the public endpoint to answer, so the test
# never subscribes against a socket that is not open yet.
echo "waiting for the hub to report ready"
ready=false
for _ in $(seq 1 30); do
	if curl -fsS "http://localhost:2019/mercure/health/ready" >/dev/null 2>&1; then
		ready=true
		break
	fi
	sleep 2
done
if [ "$ready" != true ]; then
	echo "the hub did not become ready" >&2
	exit 1
fi

echo "waiting for the hub to accept connections on ${audience}"
listening=false
for _ in $(seq 1 30); do
	# Any HTTP status means the listener is up, including the 401 that answers a
	# request without a token. A refused connection makes curl fail instead.
	if curl -sS -o /dev/null --max-time 2 "$audience" 2>/dev/null; then
		listening=true
		break
	fi
	sleep 1
done
if [ "$listening" != true ]; then
	echo "the hub did not accept connections on ${audience}" >&2
	exit 1
fi

# Mint an access token with the mercure-token command of the image.
mint() {
	printf '%s' "$key" | docker exec -i "$container" caddy mercure-token \
		--iss "$issuer" --aud "$audience" --key @- "$@"
}

publisher_jwt=$(mint --publish '*')
subscriber_jwt=$(mint --subscribe '*')

echo "opening an SSE subscription"
# Dump the response headers so the test can wait for the 200 that marks the
# subscription as established. Publishing before that point would let the
# subscriber miss the live update on a slow runner.
curl -sN --max-time 60 -D "$sse_headers" --get \
	--data-urlencode "match=$topic" \
	"$audience" \
	-H "Authorization: Bearer $subscriber_jwt" >"$sse_out" &
subscriber=$!

connected=false
for _ in $(seq 1 30); do
	if grep -q '^HTTP/[0-9.]* 200' "$sse_headers" 2>/dev/null; then
		connected=true
		break
	fi
	sleep 1
done
if [ "$connected" != true ]; then
	echo "the subscriber did not connect" >&2
	exit 1
fi

echo "publishing an update"
curl -fsS -X POST "$audience" \
	-H "Authorization: Bearer $publisher_jwt" \
	--data-urlencode "topic=$topic" \
	--data-urlencode 'data=hello' >/dev/null

# Poll for the update instead of sleeping a fixed time. The update crosses
# MongoDB and the change stream, so the delay is not deterministic on a busy
# runner.
received=false
for _ in $(seq 1 30); do
	if grep -q 'data: hello' "$sse_out"; then
		received=true
		break
	fi
	sleep 1
done

kill "$subscriber" 2>/dev/null || true
wait "$subscriber" 2>/dev/null || true
subscriber=

if [ "$received" != true ]; then
	echo "the subscriber did not receive the update" >&2
	cat "$sse_out" >&2
	exit 1
fi

echo "smoke test passed"
