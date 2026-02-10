/**
 * Fetch with automatic retry for transient failures.
 * Only retries on network errors and 5xx status codes (not 4xx).
 */
export async function fetchWithRetry(
  url: string,
  init?: RequestInit,
  opts?: { retries?: number; backoffMs?: number }
): Promise<Response> {
  const { retries = 3, backoffMs = 500 } = opts ?? {};

  let lastError: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, init);
      // Don't retry client errors (4xx), only server errors (5xx)
      if (response.status < 500 || attempt === retries) {
        return response;
      }
      lastError = new Error(`Server error: ${response.status}`);
    } catch (err) {
      // Network error — retry unless last attempt
      lastError = err;
      if (attempt === retries) break;
    }
    // Exponential backoff
    await new Promise((r) => setTimeout(r, backoffMs * Math.pow(2, attempt)));
  }

  throw lastError;
}
