import { expect, test } from "bun:test";

test("container builds the checksum-pinned official stable daemon on the same Bun/Debian ABI", async () => {
  const dockerfile = await Bun.file(new URL("../../Dockerfile", import.meta.url)).text();
  expect(dockerfile).toContain("FROM oven/bun:1.4.4 AS transmission");
  expect(dockerfile).toContain("ARG TRANSMISSION_BUILD_JOBS=2");
  expect(dockerfile).not.toMatch(/TR_PEER_ID_PREFIX|TR_USER_AGENT_PREFIX/);
  expect(dockerfile).toContain("https://github.com/transmission/transmission/releases/download/4.1.4/transmission-4.1.4.tar.xz");
  const checksum = dockerfile.indexOf("86e7aa020cdf64826447034bc85f4d3b7c848d8c0891d9dc9b9fddfb2c805d22");
  const extraction = dockerfile.indexOf("tar -xJf /tmp/transmission.tar.xz");
  expect(checksum).toBeGreaterThan(0);
  expect(checksum).toBeLessThan(extraction);
  expect(dockerfile).toContain("sha256sum --check --strict");
  expect(dockerfile.split("FROM oven/bun:1.4.4 AS transmission")[0]).toContain("COPY src ./src");
  expect(dockerfile).toContain('--target transmission-daemon --parallel "${TRANSMISSION_BUILD_JOBS}"');
  for (const flag of ["ENABLE_DAEMON=ON", "ENABLE_GTK=OFF", "ENABLE_QT=OFF", "ENABLE_MAC=OFF",
    "ENABLE_UTILS=OFF", "ENABLE_TESTS=OFF", "INSTALL_WEB=OFF", "WITH_SYSTEMD=OFF"])
    expect(dockerfile).toContain(`-D${flag}`);
  const runtime = dockerfile.split("FROM oven/bun:1.4.4\n").at(-1)!;
  expect(runtime).toContain("COPY --from=transmission /opt/transmission/ /usr/local/");
  expect(runtime).toContain("RUN transmission-daemon --version");
  expect(runtime).not.toMatch(/transmission-daemon[=~]|beta|build-essential|cmake|ninja-build/);
  expect(runtime).toContain('CMD ["bun", "run", "scripts/container.ts", "/books"]');
});
