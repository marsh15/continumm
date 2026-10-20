import { createGitHubRecoveryClient } from "./github_api.ts";

Deno.test("GitHub adapter uses App JWT delivery pagination and redelivery endpoints", async () => {
  const { keyPair, pem } = await generatePem("pkcs1");
  const appId = "123456";
  const calls: Array<{ url: URL; method: string; jwt: string }> = [];
  const client = createGitHubRecoveryClient({
    appId,
    privateKeyPem: pem,
    now: () => Date.parse("2026-10-20T12:00:00.000Z"),
    fetcher: async (input, init) => {
      const url = new URL(String(input));
      const headers = new Headers(init?.headers);
      const jwt = headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
      calls.push({ url, method: init?.method ?? "GET", jwt });
      assert(
        headers.get("x-github-api-version") === "2026-03-10",
        "API version missing",
      );
      const [headerPart, payloadPart, signaturePart] = jwt.split(".");
      if (!headerPart || !payloadPart || !signaturePart) {
        throw new Error("App JWT is malformed");
      }
      const header = JSON.parse(decodeBase64Url(headerPart)) as Record<
        string,
        unknown
      >;
      const payload = JSON.parse(decodeBase64Url(payloadPart)) as Record<
        string,
        unknown
      >;
      assert(
        header.alg === "RS256" && payload.iss === appId,
        "App JWT claims are invalid",
      );
      const valid = await crypto.subtle.verify(
        { name: "RSASSA-PKCS1-v1_5" },
        keyPair.publicKey,
        decodeBase64UrlBytes(signaturePart).buffer as ArrayBuffer,
        new TextEncoder().encode(`${headerPart}.${payloadPart}`),
      );
      assert(valid, "App JWT signature invalid");
      if (init?.method === "POST") return new Response(null, { status: 202 });
      assert(url.pathname === "/app/hook/deliveries", "wrong list endpoint");
      if (!url.searchParams.has("cursor")) {
        return new Response(
          JSON.stringify([{
            id: 123456789,
            guid: "11111111-1111-4111-8111-111111111111",
            delivered_at: "2026-10-20T11:00:00Z",
            status_code: 500,
          }]),
          {
            status: 200,
            headers: {
              link:
                '<https://api.github.com/app/hook/deliveries?cursor=next-token&per_page=100>; rel="next"',
            },
          },
        );
      }
      return new Response("[]", { status: 200 });
    },
  });

  const first = await client.listFailed(null);
  assert(
    first.deliveries.length === 1 && first.nextCursor === "next-token",
    "list page was not parsed",
  );
  const second = await client.listFailed(first.nextCursor);
  assert(second.deliveries.length === 0, "second cursor page was not read");
  const attempt = await client.redeliver(first.deliveries[0]!.id);
  assert(attempt.status === 202, "202 was not preserved");
  assert(
    calls[0]?.url.searchParams.get("per_page") === "100",
    "page size not bounded",
  );
  assert(
    calls[1]?.url.searchParams.get("cursor") === "next-token",
    "opaque cursor not followed",
  );
  assert(
    calls[2]?.url.pathname === "/app/hook/deliveries/123456789/attempts",
    "wrong redelivery route",
  );
  assert(calls[2]?.method === "POST", "redelivery method must be POST");
});

Deno.test("GitHub rate-limit response returns reset time without another request", async () => {
  let calls = 0;
  const { pem } = await generatePem();
  const client = createGitHubRecoveryClient({
    appId: "123456",
    privateKeyPem: pem,
    now: () => 1_800_000_000_000,
    fetcher: async () => {
      calls += 1;
      return new Response(null, {
        status: 429,
        headers: { "retry-after": "30" },
      });
    },
  });
  const result = await client.listFailed(null);
  assert(calls === 1, "rate-limited request count mismatch");
  assert(
    result.rateLimitedUntil === 1_800_000_030_000,
    "Retry-After was not parsed",
  );
  assert(
    result.deliveries.length === 0,
    "rate-limited response returned deliveries",
  );
});

async function generatePem(
  format: "pkcs1" | "pkcs8" = "pkcs8",
): Promise<{ keyPair: CryptoKeyPair; pem: string }> {
  const keyPair = await crypto.subtle.generateKey(
    {
      name: "RSASSA-PKCS1-v1_5",
      modulusLength: 2048,
      publicExponent: new Uint8Array([1, 0, 1]),
      hash: "SHA-256",
    },
    true,
    ["sign", "verify"],
  );
  const privateBytes = new Uint8Array(
    await crypto.subtle.exportKey("pkcs8", keyPair.privateKey),
  );
  if (format === "pkcs1") {
    let offset = readLength(privateBytes, 1).content;
    offset = skipTlv(privateBytes, offset);
    offset = skipTlv(privateBytes, offset);
    if (privateBytes[offset] !== 0x04) {
      throw new Error("test_pkcs8_fixture_invalid");
    }
    const octets = readLength(privateBytes, offset + 1);
    const pkcs1 = privateBytes.slice(
      octets.content,
      octets.content + octets.length,
    );
    return {
      keyPair,
      pem: `-----BEGIN RSA PRIVATE KEY-----\n${
        base64(pkcs1)
      }\n-----END RSA PRIVATE KEY-----`,
    };
  }
  return {
    keyPair,
    pem: `-----BEGIN PRIVATE KEY-----\n${
      base64(privateBytes)
    }\n-----END PRIVATE KEY-----`,
  };
}

function skipTlv(bytes: Uint8Array, offset: number): number {
  const value = readLength(bytes, offset + 1);
  return value.content + value.length;
}

function readLength(
  bytes: Uint8Array,
  offset: number,
): { length: number; content: number } {
  const first = bytes[offset] ?? 0;
  if ((first & 0x80) === 0) return { length: first, content: offset + 1 };
  const count = first & 0x7f;
  let length = 0;
  for (let index = 0; index < count; index++) {
    length = length * 256 + (bytes[offset + 1 + index] ?? 0);
  }
  return { length, content: offset + 1 + count };
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}

function base64(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

function decodeBase64Url(value: string): string {
  return new TextDecoder().decode(decodeBase64UrlBytes(value));
}

function decodeBase64UrlBytes(value: string): Uint8Array {
  const padded = value.replace(/-/g, "+").replace(/_/g, "/") +
    "===".slice((value.length + 3) % 4);
  const binary = atob(padded);
  return Uint8Array.from(binary, (character) => character.charCodeAt(0));
}
