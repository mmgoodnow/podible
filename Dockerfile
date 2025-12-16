FROM oven/bun:latest

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg unzip \
  && rm -rf /var/lib/apt/lists/*

COPY package.json bun.lock ./
RUN bun install --production

COPY server.ts ./

EXPOSE 80

CMD ["bun", "run", "server.ts", "/books"]
