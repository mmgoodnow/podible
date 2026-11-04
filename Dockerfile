FROM oven/bun:1.4.4 AS frontend

WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY web ./web
COPY src ./src
COPY scripts/build-web.ts ./scripts/build-web.ts
RUN bun run build

FROM oven/bun:1.4.4 AS transmission

ARG TRANSMISSION_BUILD_JOBS=2

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential ca-certificates cmake curl libcurl4-openssl-dev \
    libdeflate-dev libevent-dev libpsl-dev libssl-dev ninja-build pkg-config xz-utils \
  && rm -rf /var/lib/apt/lists/*

# Use the complete official release asset, including its pinned third-party sources.
RUN curl --fail --location --proto '=https' --tlsv1.2 --connect-timeout 15 --max-time 120 \
    https://github.com/transmission/transmission/releases/download/4.1.4/transmission-4.1.4.tar.xz \
    --output /tmp/transmission.tar.xz \
  && echo '86e7aa020cdf64826447034bc85f4d3b7c848d8c0891d9dc9b9fddfb2c805d22  /tmp/transmission.tar.xz' | sha256sum --check --strict \
  && mkdir /tmp/transmission \
  && tar -xJf /tmp/transmission.tar.xz -C /tmp/transmission --strip-components=1 \
  && cmake -S /tmp/transmission -B /tmp/transmission-build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DENABLE_DAEMON=ON \
    -DENABLE_GTK=OFF -DENABLE_QT=OFF -DENABLE_MAC=OFF \
    -DENABLE_UTILS=OFF -DENABLE_CLI=OFF -DENABLE_TESTS=OFF -DENABLE_NLS=OFF \
    -DINSTALL_WEB=OFF -DREBUILD_WEB=OFF -DINSTALL_DOC=OFF -DINSTALL_LIB=OFF \
    -DWITH_SYSTEMD=OFF -DWITH_CRYPTO=openssl -DRUN_CLANG_TIDY=OFF \
    -DUSE_SYSTEM_EVENT2=ON -DUSE_SYSTEM_DEFLATE=ON -DUSE_SYSTEM_PSL=ON \
  && cmake --build /tmp/transmission-build --target transmission-daemon --parallel "${TRANSMISSION_BUILD_JOBS}" \
  && install -Dm755 /tmp/transmission-build/daemon/transmission-daemon /opt/transmission/bin/transmission-daemon \
  && strip /opt/transmission/bin/transmission-daemon \
  && install -Dm644 /tmp/transmission/COPYING /opt/transmission/share/licenses/transmission/COPYING

FROM oven/bun:1.4.4

WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    ca-certificates ffmpeg unzip wamerican libcurl4t64 libdeflate0 \
    libevent-2.1-7t64 libpsl5t64 libssl3t64 libstdc++6 \
  && rm -rf /var/lib/apt/lists/*

COPY --from=transmission /opt/transmission/ /usr/local/
RUN transmission-daemon --version

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
