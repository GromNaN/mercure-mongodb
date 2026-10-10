# syntax=docker/dockerfile:1
FROM caddy:2-alpine

ARG TARGETPLATFORM

LABEL org.opencontainers.image.title="mercure-mongodb"
LABEL org.opencontainers.image.description="Mercure hub with a MongoDB transport"
LABEL org.opencontainers.image.url="https://github.com/GromNaN/mercure-mongodb"
LABEL org.opencontainers.image.documentation="https://github.com/GromNaN/mercure-mongodb#readme"
LABEL org.opencontainers.image.source="https://github.com/GromNaN/mercure-mongodb"
# The image bundles three works under three licenses: this transport (MIT), the
# Mercure hub (AGPL-3.0-or-later) and Caddy (Apache-2.0). The texts ship in
# /licenses.
LABEL org.opencontainers.image.licenses="MIT AND AGPL-3.0-or-later AND Apache-2.0"

# The hub binary is built by GoReleaser for each target platform and copied into
# the build context under ${TARGETPLATFORM}.
COPY ${TARGETPLATFORM}/mercure-mongodb /usr/bin/caddy
COPY Caddyfile /etc/caddy/Caddyfile

# License texts for the works bundled in the image.
COPY LICENSE /licenses/MIT.txt
COPY LICENSES/ /licenses/

# Transport-aware readiness check on the Caddy admin API (localhost:2019 by default).
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=5 \
	CMD ["wget", "-q", "--spider", "http://127.0.0.1:2019/mercure/health/ready"]
