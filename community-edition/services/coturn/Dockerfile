FROM docker.io/mozillareality/coturn@sha256:8380269c7bb2dc369f4126251199f0d603711debe8537b22cb7be470a50c51ce

# Keep the proven runtime image and replace only its credential-leaking entrypoint.
COPY --chmod=0755 entrypoint.sh /entrypoint.sh
