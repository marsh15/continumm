import { Pool } from "@db/postgres";
import type { InboxDelivery, PersistOutcome } from "./handler.ts";

const STATEMENT_TIMEOUT_MS = 5_500;
const LOCK_TIMEOUT_MS = 1_000;

export function createPostgresInboxWriter(pool: Pool) {
  return async (
    delivery: InboxDelivery,
    signal: AbortSignal,
  ): Promise<PersistOutcome> => {
    throwIfAborted(signal);
    const pendingClient = pool.connect();
    let client: Awaited<typeof pendingClient>;
    try {
      client = await waitForSignal(pendingClient, signal);
    } catch (error) {
      if (signal.aborted) {
        void pendingClient.then((lateClient) => lateClient.release()).catch(
          () => undefined,
        );
      }
      throw error;
    }

    try {
      throwIfAborted(signal);
      await queryWithSignal(client.queryArray("BEGIN"), signal);
      await queryWithSignal(
        client.queryArray(
          `SET LOCAL statement_timeout = '${STATEMENT_TIMEOUT_MS}ms'`,
        ),
        signal,
      );
      await queryWithSignal(
        client.queryArray(`SET LOCAL lock_timeout = '${LOCK_TIMEOUT_MS}ms'`),
        signal,
      );
      throwIfAborted(signal);

      const inserted = await queryWithSignal(
        client.queryObject<{ delivery_id: string }>`
        INSERT INTO webhook_private.github_delivery_inbox
          (github_app_id, delivery_id, event_type, action, payload_sha256, raw_body)
        VALUES (
          ${delivery.appId}::bigint,
          ${delivery.deliveryId}::uuid,
          ${delivery.eventType},
          ${delivery.action},
          ${delivery.payloadSha256},
          ${delivery.rawBody}
        )
        ON CONFLICT (github_app_id, delivery_id) DO NOTHING
        RETURNING delivery_id
      `,
        signal,
      );
      throwIfAborted(signal);
      if (inserted.rows.length > 0) {
        await commit(client, signal);
        return "inserted";
      }

      const existing = await queryWithSignal(
        client.queryObject<{ payload_digest_hex: string }>`
        SELECT encode(payload_sha256, 'hex') AS payload_digest_hex
        FROM webhook_private.github_delivery_inbox
        WHERE github_app_id = ${delivery.appId}::bigint
          AND delivery_id = ${delivery.deliveryId}::uuid
      `,
        signal,
      );
      throwIfAborted(signal);
      const row = existing.rows[0];
      if (!row) throw new Error("inbox_conflict_row_missing");
      const outcome = row.payload_digest_hex === toHex(delivery.payloadSha256)
        ? "duplicate"
        : "conflict";
      await commit(client, signal);
      return outcome;
    } catch (error) {
      // Closing the connection rolls back any open transaction and prevents a
      // client with uncertain protocol state from re-entering the pool.
      await client.end().catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  };
}

export function createLazyPostgresPool(connectionString: string): Pool {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    throw new Error("ingress_database_url_invalid");
  }
  if (url.protocol !== "postgres:" && url.protocol !== "postgresql:") {
    throw new Error("ingress_database_url_invalid");
  }
  const sslModes = url.searchParams.getAll("sslmode");
  if (
    sslModes.length !== 1 ||
    !["require", "verify-ca", "verify-full"].includes(sslModes[0] ?? "")
  ) {
    throw new Error("ingress_database_tls_required");
  }
  if (
    !url.hostname || !url.username || !url.password ||
    url.pathname.length < 2 || url.searchParams.size !== 1 || url.hash
  ) {
    throw new Error("ingress_database_url_invalid");
  }

  let user: string;
  let password: string;
  let database: string;
  try {
    user = decodeURIComponent(url.username);
    password = decodeURIComponent(url.password);
    database = decodeURIComponent(url.pathname.slice(1));
  } catch {
    throw new Error("ingress_database_url_invalid");
  }
  if (!database || database.includes("/")) {
    throw new Error("ingress_database_url_invalid");
  }

  try {
    return new Pool(
      {
        applicationName: "continumm-github-ingress",
        connection: { attempts: 1, interval: 500 },
        database,
        hostname: url.hostname,
        password,
        port: url.port ? Number(url.port) : 5432,
        tls: { enabled: true, enforce: true, caCertificates: [] },
        user,
      },
      1,
      true,
    );
  } catch {
    throw new Error("ingress_database_pool_invalid");
  }
}

async function commit(
  client: Awaited<ReturnType<Pool["connect"]>>,
  signal: AbortSignal,
): Promise<void> {
  throwIfAborted(signal);
  await queryWithSignal(client.queryArray("COMMIT"), signal);
}

function queryWithSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  return waitForSignal(promise, signal);
}

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw new Error("ingress_request_aborted");
}

function waitForSignal<T>(
  promise: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) {
    return Promise.reject(new Error("ingress_request_aborted"));
  }
  return new Promise<T>((resolve, reject) => {
    const cleanup = () => signal.removeEventListener("abort", onAbort);
    const onAbort = () => {
      cleanup();
      reject(new Error("ingress_request_aborted"));
    };
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        cleanup();
        resolve(value);
      },
      (error: unknown) => {
        cleanup();
        reject(error);
      },
    );
  });
}

function toHex(bytes: Uint8Array): string {
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}
