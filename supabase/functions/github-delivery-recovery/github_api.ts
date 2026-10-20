import type {
  Delivery,
  DeliveryPage,
  GitHubRecoveryClient,
} from "./handler.ts";

const API_ROOT = "https://api.github.com";
const API_VERSION = "2026-03-10";
const REQUEST_TIMEOUT_MS = 10_000;

export function createGitHubRecoveryClient(config: {
  appId: string;
  privateKeyPem: string;
  fetcher?: typeof fetch;
  now?: () => number;
}): GitHubRecoveryClient {
  const fetcher = config.fetcher ?? fetch;
  const now = config.now ?? Date.now;
  return {
    async listFailed(cursor) {
      const url = new URL("/app/hook/deliveries", API_ROOT);
      url.searchParams.set("per_page", "100");
      if (cursor) url.searchParams.set("cursor", cursor);
      const api = await request(
        fetcher,
        url,
        await createAppJwt(config, now),
      );
      const response = api.response;
      if (response.status === 403 || response.status === 429) {
        const retryAt = rateLimitUntil(response.headers, now());
        await response.body?.cancel().catch(() => undefined);
        api.close();
        if (retryAt !== null) {
          return {
            deliveries: [],
            nextCursor: null,
            rateLimitedUntil: retryAt,
          };
        }
      }
      if (!response.ok) {
        await response.body?.cancel().catch(() => undefined);
        api.close();
        throw new Error("github_delivery_list_failed");
      }
      let body: unknown;
      try {
        body = await readJsonBounded(response, 2 * 1024 * 1024, api.signal);
      } finally {
        api.close();
      }
      if (!Array.isArray(body)) throw new Error("github_delivery_list_invalid");
      const deliveries = body.map(parseDelivery).filter((
        item,
      ): item is Delivery => item !== null);
      return {
        deliveries,
        nextCursor: nextPageCursor(response.headers.get("link")),
        rateLimitedUntil: null,
      };
    },
    async redeliver(deliveryId) {
      if (!/^[1-9][0-9]{0,19}$/.test(deliveryId)) {
        throw new Error("github_delivery_id_invalid");
      }
      const api = await request(
        fetcher,
        new URL(`/app/hook/deliveries/${deliveryId}/attempts`, API_ROOT),
        await createAppJwt(config, now),
        "POST",
      );
      const response = api.response;
      const retryAt = response.status === 403 || response.status === 429
        ? rateLimitUntil(response.headers, now())
        : null;
      await response.body?.cancel().catch(() => undefined);
      api.close();
      return { status: response.status, retryAt };
    },
  };
}

async function request(
  fetcher: typeof fetch,
  url: URL,
  jwt: string,
  method = "GET",
): Promise<{ response: Response; signal: AbortSignal; close: () => void }> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const response = await fetcher(url, {
      method,
      headers: {
        accept: "application/vnd.github+json",
        authorization: `Bearer ${jwt}`,
        "x-github-api-version": API_VERSION,
      },
      redirect: "error",
      signal: controller.signal,
    });
    return {
      response,
      signal: controller.signal,
      close: () => clearTimeout(timeout),
    };
  } catch (error) {
    clearTimeout(timeout);
    throw error;
  }
}

async function createAppJwt(
  config: { appId: string; privateKeyPem: string },
  now: () => number,
): Promise<string> {
  if (!/^[1-9][0-9]{0,18}$/.test(config.appId)) {
    throw new Error("github_app_id_invalid");
  }
  const issuedAt = Math.floor(now() / 1000) - 30;
  const header = base64Url(
    new TextEncoder().encode(JSON.stringify({ alg: "RS256", typ: "JWT" })),
  );
  const payload = base64Url(new TextEncoder().encode(JSON.stringify({
    iat: issuedAt,
    exp: issuedAt + 540,
    iss: config.appId,
  })));
  const signingInput = `${header}.${payload}`;
  const key = await importPrivateKey(config.privateKeyPem);
  const signature = await crypto.subtle.sign(
    { name: "RSASSA-PKCS1-v1_5" },
    key,
    new TextEncoder().encode(signingInput),
  );
  return `${signingInput}.${base64Url(new Uint8Array(signature))}`;
}

async function importPrivateKey(pem: string): Promise<CryptoKey> {
  const normalized = pem.replace(/\\n/g, "\n").trim();
  const match = normalized.match(
    /^-----BEGIN (PRIVATE KEY|RSA PRIVATE KEY)-----\s+([\s\S]+?)\s+-----END (?:PRIVATE KEY|RSA PRIVATE KEY)-----$/,
  );
  if (!match?.[1] || !match[2]) throw new Error("github_private_key_invalid");
  const binary = atob(match[2].replace(/\s/g, ""));
  let bytes: Uint8Array<ArrayBuffer> = Uint8Array.from(
    binary,
    (character) => character.charCodeAt(0),
  );
  if (match[1] === "RSA PRIVATE KEY") bytes = wrapPkcs1AsPkcs8(bytes);
  return await crypto.subtle.importKey(
    "pkcs8",
    bytes,
    { name: "RSASSA-PKCS1-v1_5", hash: "SHA-256" },
    false,
    ["sign"],
  );
}

function wrapPkcs1AsPkcs8(pkcs1: Uint8Array): Uint8Array<ArrayBuffer> {
  const algorithm = new Uint8Array([
    0x30,
    0x0d,
    0x06,
    0x09,
    0x2a,
    0x86,
    0x48,
    0x86,
    0xf7,
    0x0d,
    0x01,
    0x01,
    0x01,
    0x05,
    0x00,
  ]);
  const version = new Uint8Array([0x02, 0x01, 0x00]);
  const octets = new Uint8Array([
    0x04,
    ...derLength(pkcs1.byteLength),
    ...pkcs1,
  ]);
  const content = new Uint8Array([...version, ...algorithm, ...octets]);
  return new Uint8Array([0x30, ...derLength(content.byteLength), ...content]);
}

function derLength(length: number): number[] {
  if (length < 128) return [length];
  const bytes: number[] = [];
  for (let value = length; value > 0; value >>>= 8) bytes.unshift(value & 0xff);
  return [0x80 | bytes.length, ...bytes];
}

function parseDelivery(value: unknown): Delivery | null {
  if (value === null || typeof value !== "object") return null;
  const row = value as Record<string, unknown>;
  if (typeof row.id !== "number" && typeof row.id !== "string") return null;
  if (typeof row.id === "number" && !Number.isSafeInteger(row.id)) return null;
  const id = String(row.id);
  if (
    !/^[1-9][0-9]{0,19}$/.test(id) ||
    typeof row.guid !== "string" ||
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(
      row.guid,
    ) ||
    typeof row.delivered_at !== "string" ||
    !Number.isFinite(Date.parse(row.delivered_at)) ||
    (row.status_code !== null &&
      (typeof row.status_code !== "number" ||
        !Number.isInteger(row.status_code) || row.status_code < 0 ||
        row.status_code > 599))
  ) return null;
  return {
    id,
    guid: row.guid.toLowerCase(),
    delivered_at: row.delivered_at,
    status_code: typeof row.status_code === "number" ? row.status_code : 0,
  };
}

function nextPageCursor(link: string | null): string | null {
  if (!link) return null;
  for (const part of link.split(",")) {
    const match = part.trim().match(/^<([^>]+)>;\s*rel="next"$/);
    if (!match?.[1]) continue;
    let url: URL;
    try {
      url = new URL(match[1]);
    } catch {
      throw new Error("github_pagination_link_invalid");
    }
    if (url.origin !== API_ROOT || url.pathname !== "/app/hook/deliveries") {
      throw new Error("github_pagination_link_invalid");
    }
    return url.searchParams.get("cursor");
  }
  return null;
}

function rateLimitUntil(headers: Headers, now: number): number | null {
  const retryAfter = headers.get("retry-after");
  if (retryAfter && /^[0-9]+$/.test(retryAfter)) {
    return now + Number(retryAfter) * 1000;
  }
  const reset = headers.get("x-ratelimit-reset");
  if (reset && /^[0-9]+$/.test(reset)) return Number(reset) * 1000;
  return null;
}

async function readJsonBounded(
  response: Response,
  limit: number,
  signal: AbortSignal,
): Promise<unknown> {
  if (!response.body) throw new Error("github_response_empty");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    for (;;) {
      const { done, value } = await readWithAbort(reader, signal);
      if (done) break;
      size += value.byteLength;
      if (size > limit) {
        await reader.cancel().catch(() => undefined);
        throw new Error("github_response_too_large");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
}

function readWithAbort(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  signal: AbortSignal,
): Promise<ReadableStreamReadResult<Uint8Array>> {
  if (signal.aborted) {
    return Promise.reject(new Error("github_request_timeout"));
  }
  return new Promise((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => {
      cleanup();
      reject(new Error("github_request_timeout"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    reader.read().then(
      (result) => {
        cleanup();
        resolve(result);
      },
      (error: unknown) => {
        cleanup();
        reject(error);
      },
    );
  });
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(
    /=+$/g,
    "",
  );
}
