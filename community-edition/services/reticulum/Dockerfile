# syntax=docker/dockerfile:1
ARG ALPINE_VERSION=3.21.7
ARG ELIXIR_VERSION=1.18.4
ARG ERLANG_VERSION=27.3.4.14

FROM hexpm/elixir:${ELIXIR_VERSION}-erlang-${ERLANG_VERSION}-alpine-${ALPINE_VERSION} AS base
RUN mix do local.hex --force, local.rebar --force

FROM base AS dev
RUN apk add --no-cache\
    # required by hex\
    git\
    # required by hex:phoenix_live_reload\
    inotify-tools
COPY container/dev-perms.pem /etc/perms.pem
COPY container/trapped-mix /usr/local/bin/trapped-mix
WORKDIR /code
HEALTHCHECK CMD wget --no-check-certificate --no-verbose --tries=1 --spider https://localhost:4000/stream-offline.png
CMD sh -c "PERMS_KEY=\"$(cat /etc/perms.pem)\" mix phx.server"

FROM base AS builder
RUN apk add --no-cache nodejs yarn git build-base
WORKDIR /build
COPY . .
RUN mix deps.get
RUN MIX_ENV=turkey mix release

FROM alpine:${ALPINE_VERSION} AS certr
RUN apk add --no-cache openssl
WORKDIR /certs
RUN openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes -keyout key.pem -out cert.pem -subj '/CN=ret' && cp cert.pem cacert.pem

FROM alpine:${ALPINE_VERSION}
RUN apk add --no-cache bash coreutils jq libstdc++ openssl \
    && mkdir -p /ret/var /storage \
    && chmod 777 /storage
WORKDIR /ret
COPY --from=builder /build/_build/turkey/rel/ret/ .
COPY --from=certr /certs/ .
COPY container/prod-run.sh /run.sh
CMD bash /run.sh
