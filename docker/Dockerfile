FROM crystallang/crystal:1.16.3-alpine AS builder

RUN apk add --no-cache sqlite-static yaml-static

ARG release

WORKDIR /invidious

# Installa le dipendenze Crystal
COPY ./shard.yml ./shard.yml
COPY ./shard.lock ./shard.lock
RUN shards install --production

# Copia i sorgenti e le risorse necessarie
COPY ./src/ ./src/
COPY ./scripts/ ./scripts/
COPY ./assets/ ./assets/
COPY ./videojs-dependencies.yml ./videojs-dependencies.yml

# Esegui i test (opzionale, puoi commentare se vuoi build più veloce)
RUN crystal spec --warnings all --link-flags "-lxml2 -llzma"

# Compila invidious: la sintassi è compatibile con /bin/sh di Alpine
RUN if [ "${release}" = "1" ]; then \
      crystal build ./src/invidious.cr \
      --release \
      --static --warnings all \
      --link-flags "-lxml2 -llzma"; \
    else \
      crystal build ./src/invidious.cr \
      --static --warnings all \
      --link-flags "-lxml2 -llzma"; \
    fi

FROM alpine:3.21

RUN apk add --no-cache rsvg-convert ttf-opensans tini tzdata

WORKDIR /invidious

# Crea utente dedicato
RUN addgroup -g 1000 -S invidious && \
    adduser -u 1000 -S invidious -G invidious

# Copia i file di configurazione e localizzazione
COPY --chown=invidious ./config/config.* ./config/
RUN mv -n config/config.example.yml config/config.yml
RUN sed -i 's/host: \(127.0.0.1\|localhost\)/host: invidious-db/' config/config.yml
COPY ./config/sql/ ./config/sql/
COPY ./locales/ ./locales/

# Copia asset e binario compilato
COPY --from=builder /invidious/assets ./assets/
COPY --from=builder /invidious/invidious .

RUN chmod o+rX -R ./assets ./config ./locales

EXPOSE 3000

USER invidious

ENTRYPOINT ["/sbin/tini", "--"]
CMD [ "/invidious/invidious" ]
