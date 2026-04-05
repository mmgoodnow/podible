export function json(value: unknown, status = 200): Response {
  return new Response(JSON.stringify(value, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

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

export function redirect(location: string, status = 303): Response {
  return new Response(null, {
    status,
    headers: {
      Location: location,
      "Cache-Control": "no-store",
    },
  });
}

export function parseId(value: string): number {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new Error("Invalid id");
  }
  return parsed;
}
