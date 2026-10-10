# syntax=docker/dockerfile:1
FROM caddy:2-alpine

ARG TARGETPLATFORM

LABEL org.opencontainers.image.title="mercure-mongodb"
LABEL org.opencontainers.image.description="Mercure hub with a MongoDB transport"
LABEL org.opencontainers.image.url="https://github.com/GromNaN/mercure-mongodb"
LABEL org.opencontainers.image.documentation="https://github.com/GromNaN/mercure-mongodb#readme"
LABEL org.opencontainers.image.source="https://github.com/GromNaN/mercure-mongodb"
# The image bundles three works under two licenses: this transport and the
# Mercure hub are AGPL-3.0-or-later, Caddy is Apache-2.0.
LABEL org.opencontainers.image.licenses="AGPL-3.0-or-later AND Apache-2.0"

# The hub binary is built by GoReleaser for each target platform and copied into
# the build context under ${TARGETPLATFORM}.
COPY ${TARGETPLATFORM}/mercure-mongodb /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile

# Transport-aware readiness check on the Caddy admin API (localhost:2019 by default).
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=5 \
	CMD ["wget", "-q", "--spider", "http://127.0.0.1:2019/mercure/health/ready"]
