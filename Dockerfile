FROM oven/bun:1.4.4 AS frontend

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY web ./web
COPY scripts/build-web.ts ./scripts/build-web.ts
RUN bun run build

FROM oven/bun:1.4.4

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg unzip wamerican transmission-daemon=4.1.0~beta2+dfsg-3+deb13u2 \
  && rm -rf /var/lib/apt/lists/*

COPY package.json bun.lock ./
RUN bun install --frozen-lockfile --production

ARG GIT_SHA
ARG GIT_COMMIT_MESSAGE

ENV GIT_COMMIT_SHA=$GIT_SHA
ENV GIT_COMMIT_MESSAGE=$GIT_COMMIT_MESSAGE

COPY server.ts ./
COPY src ./src
COPY scripts ./scripts
COPY --from=frontend /app/web/dist ./web/dist
COPY podible.png ./

RUN bun run validate:runtime-scripts

ENV NODE_ENV=production
ENV CONFIG_DIR=/config

EXPOSE 80
EXPOSE 51413/tcp 51413/udp

CMD ["bun", "run", "scripts/container.ts", "/books"]
