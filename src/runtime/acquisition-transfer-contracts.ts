import { z } from "zod";

export const transferScope = z.strictObject({ media: z.enum(["movie", "tv"]), id: z.number().int().positive().safe() });
const bytes = z.number().int().nonnegative().safe();
export const transferEvent = z.object({ sample: z.object({
  bytesDone: bytes, sizeBytes: bytes.positive(), bytesPerSecond: bytes,
  etaSeconds: bytes.nullable(),
}).nullable() });
export type AcquisitionTransferScope = z.infer<typeof transferScope>;
export type AcquisitionTransferSample = NonNullable<z.infer<typeof transferEvent>["sample"]>;
export const transferIntervalMs = 5_000;
