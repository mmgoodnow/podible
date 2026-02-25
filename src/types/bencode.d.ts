declare module "bencode" {
  const bencode: {
    decode(input: Uint8Array | ArrayBuffer | Buffer): unknown;
    encode(input: unknown): Uint8Array;
    byteLength(input: unknown): number;
    encodingLength(input: unknown): number;
  };
  export default bencode;
}
