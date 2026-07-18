FROM oven/bun:canary

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg unzip wamerican \
  && rm -rf /var/lib/apt/lists/*

COPY package.json bun.lock ./
RUN bun install --production

ARG GIT_SHA
ARG GIT_COMMIT_MESSAGE

ENV GIT_COMMIT_SHA=$GIT_SHA
ENV GIT_COMMIT_MESSAGE=$GIT_COMMIT_MESSAGE

COPY server.ts ./
COPY src ./src
COPY scripts ./scripts
COPY podible.png ./

RUN bun run validate:runtime-scripts

ENV NODE_ENV=production
ENV CONFIG_DIR=/config

EXPOSE 80

CMD ["bun", "run", "server.ts", "/books"]
