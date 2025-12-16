function authorize(request: Request, key: string): boolean {
  const url = new URL(request.url);
  const queryKey = url.searchParams.get("key");
  if (queryKey && queryKey.trim() === key) return true;
  const header = request.headers.get("authorization");
  if (header?.toLowerCase().startsWith("bearer ")) {
    const token = header.slice("bearer ".length).trim();
    if (token === key) return true;
  }
  const apiKeyHeader = request.headers.get("x-api-key");
  if (apiKeyHeader && apiKeyHeader.trim() === key) return true;
  return false;
}

export { authorize };
