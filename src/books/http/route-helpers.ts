import { HTTPException } from "hono/http-exception";

function acceptsBrotli(request: Request): boolean {
  return (request.headers.get("accept-encoding")?.toLowerCase() ?? "").includes("br");
}

export async function maybeCompressBrotli(request: Request, response: Response): Promise<Response> {
  if (!acceptsBrotli(request) || !response.body || response.headers.has("Content-Encoding")) {
    return response;
  }

  const headers = new Headers(response.headers);
  headers.set("Content-Encoding", "br");
  headers.append("Vary", "Accept-Encoding");
  headers.delete("Content-Length");

  return new Response(response.body.pipeThrough(new CompressionStream("brotli")), {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export async function jsonResponse(
  request: Request,
  value: unknown,
  status = 200,
  contentType = "application/json; charset=utf-8"
): Promise<Response> {
  return maybeCompressBrotli(
    request,
    new Response(JSON.stringify(value, null, 2), {
      status,
      headers: {
        "Content-Type": contentType,
        "Cache-Control": "no-store",
      },
    })
  );
}

export function formString(
  body: Record<string, string | File | (string | File)[]>,
  key: string
): string {
  const value = body[key];
  if (typeof value === "string") return value;
  if (Array.isArray(value)) {
    const first = value[0];
    return typeof first === "string" ? first : "";
  }
  return "";
}

export function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new HTTPException(400, { message: "Invalid id" });
  }
  return parsed;
}
