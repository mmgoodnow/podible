import net from "node:net";
import os from "node:os";
import { createHash, randomBytes } from "node:crypto";
import bencode from "bencode";

/** Minimal single-piece seed for the offline lab, never a public tracker/client. */
export async function startFixturePeer() {
  const ip = Object.values(os.networkInterfaces())
    .flat()
    .find((item) => item?.family === "IPv4" && !item.internal)?.address;
  if (!ip) throw new Error("Private lab interface not found");
  const bytes = randomBytes(12_000);
  const name = `transfer-${randomBytes(5).toString("hex")}.bin`;
  const info = {
    name,
    length: bytes.length,
    "piece length": 16384,
    pieces: createHash("sha1").update(bytes).digest(),
    private: 1,
  };
  const hash = createHash("sha1").update(bencode.encode(info)).digest();
  let requests = 0;
  let announces = 0;
  const sockets = new Set<net.Socket>();
  const peer = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
    let buffer = Buffer.alloc(0);
    let handshaken = false;
    const message = (id: number, data = Buffer.alloc(0)) => {
      const header = Buffer.alloc(5);
      header.writeUInt32BE(1 + data.length, 0);
      header[4] = id;
      socket.write(Buffer.concat([header, data]));
    };
    socket.on("data", (data) => {
      buffer = Buffer.concat([
        buffer,
        typeof data === "string" ? Buffer.from(data) : data,
      ]);
      if (!handshaken) {
        if (buffer.length < 68) return;
        if (buffer[0] !== 19 || !buffer.subarray(28, 48).equals(hash)) {
          socket.destroy();
          return;
        }
        const handshake = Buffer.concat([
          Buffer.from([19]),
          Buffer.from("BitTorrent protocol"),
          Buffer.alloc(8),
          hash,
          Buffer.from("-PL0001-123456789012"),
        ]);
        socket.write(handshake);
        message(5, Buffer.from([0x80]));
        message(1);
        buffer = buffer.subarray(68);
        handshaken = true;
      }
      while (buffer.length >= 4) {
        const length = buffer.readUInt32BE(0);
        if (length > 64 * 1024) {
          socket.destroy();
          return;
        }
        if (buffer.length < 4 + length) return;
        const frame = buffer.subarray(4, 4 + length);
        buffer = buffer.subarray(4 + length);
        if (!length || frame[0] !== 6 || frame.length !== 13) continue;
        const index = frame.readUInt32BE(1);
        const begin = frame.readUInt32BE(5);
        const size = frame.readUInt32BE(9);
        if (index !== 0 || size > 16384 || begin + size > bytes.length) {
          socket.destroy();
          return;
        }
        requests++;
        const offset = Buffer.alloc(8);
        offset.writeUInt32BE(index, 0);
        offset.writeUInt32BE(begin, 4);
        message(
          7,
          Buffer.concat([offset, bytes.subarray(begin, begin + size)]),
        );
      }
    });
  });
  await new Promise<void>((resolve, reject) => {
    peer.once("error", reject);
    peer.listen(0, "0.0.0.0", resolve);
  });
  const port = (peer.address() as net.AddressInfo).port;
  const compact = Buffer.alloc(6);
  ip.split(".").forEach((part, index) => {
    compact[index] = Number(part);
  });
  compact.writeUInt16BE(port, 4);
  const tracker = Bun.serve({
    hostname: "0.0.0.0",
    port: 0,
    fetch(request) {
      if (new URL(request.url).pathname !== "/announce")
        return new Response("", { status: 404 });
      announces++;
      return new Response(
        bencode.encode({
          interval: 30,
          complete: 1,
          incomplete: 0,
          peers: compact,
        }),
        { headers: { "Content-Type": "application/x-bittorrent" } },
      );
    },
  });
  const metainfo = new Uint8Array(
    bencode.encode({ announce: `http://${ip}:${tracker.port}/announce`, info }),
  );
  return {
    bytes,
    name,
    metainfo,
    stats: () => ({ requests, announces }),
    close: async () => {
      tracker.stop(true);
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => peer.close(() => resolve()));
    },
  };
}
