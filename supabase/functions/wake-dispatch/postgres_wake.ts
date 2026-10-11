import { Pool } from "@db/postgres";
import type { PoolClient } from "@db/postgres";
import type {
  DispatchAttempt,
  DispatchOutcomeKind,
  DispatchStore,
} from "./handler.ts";

export interface WakeDatabaseTimeouts {
  acquireMs: number;
  queryMs: number;
  closeMs: number;
}

const DEFAULT_TIMEOUTS: WakeDatabaseTimeouts = {
  acquireMs: 8_000,
  queryMs: 6_000,
  closeMs: 1_000,
};

export function createPostgresWakeStore(
  pool: Pool,
  timeouts: WakeDatabaseTimeouts = DEFAULT_TIMEOUTS,
): DispatchStore {
  return {
    async consumeTicket(
      ticketId: string,
      ticketValue: string,
    ): Promise<DispatchAttempt | null> {
      const result = await runDatabaseQuery(
        pool,
        timeouts,
        (client) =>
          client.queryObject<{
            authorized: boolean;
            attempt_id: string | null;
            wake_id: string | null;
            generation: string | bigint | number | null;
            reason: string | null;
          }>`
          SELECT *
          FROM wake_private.consume_dispatch_ticket(
            ${ticketId}::uuid,
            ${ticketValue}::text
          )
        `,
      );
      const row = result.rows[0];
      if (!row || !row.authorized) return null;
      if (
        !row.attempt_id || !row.wake_id || row.generation === null ||
        row.reason !== "pending_inbox"
      ) {
        throw new Error("wake_dispatch_database_result_invalid");
      }
      if (
        typeof row.generation === "number" &&
        !Number.isSafeInteger(row.generation)
      ) {
        throw new Error("wake_dispatch_database_result_invalid");
      }
      const generation = String(row.generation);
      if (
        !/^[1-9][0-9]{0,18}$/.test(generation) ||
        BigInt(generation) > 9_223_372_036_854_775_807n
      ) {
        throw new Error("wake_dispatch_database_result_invalid");
      }
      return {
        attemptId: row.attempt_id,
        wakeId: row.wake_id,
        generation,
        reason: "pending_inbox",
      };
    },

    async recordResult(
      attempt: DispatchAttempt,
      outcome: DispatchOutcomeKind,
      httpStatus: number | null,
    ): Promise<boolean> {
      const result = await runDatabaseQuery(
        pool,
        timeouts,
        (client) =>
          client.queryObject<{ recorded: boolean }>`
          SELECT recorded
          FROM wake_private.record_dispatch_result(
            ${attempt.attemptId}::uuid,
            ${attempt.generation}::bigint,
            ${outcome}::text,
            ${httpStatus}::smallint
          )
        `,
      );
      const row = result.rows[0];
      if (!row) throw new Error("wake_dispatch_database_result_missing");
      return row.recorded;
    },
  };
}

async function runDatabaseQuery<T>(
  pool: Pool,
  timeouts: WakeDatabaseTimeouts,
  query: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await acquireClient(pool, timeouts);
  let reusable = false;
  try {
    const result = await withTimeout(
      query(client),
      timeouts.queryMs,
      "wake_database_query_timeout",
    );
    reusable = true;
    return result;
  } catch {
    throw new Error("wake_database_operation_failed");
  } finally {
    if (reusable) client.release();
    else await closeAndRelease(client, timeouts.closeMs);
  }
}

async function acquireClient(
  pool: Pool,
  timeouts: WakeDatabaseTimeouts,
): Promise<PoolClient> {
  let abandoned = false;
  let acquired: PoolClient | null = null;
  const pending = pool.connect().then((client) => {
    acquired = client;
    if (abandoned) {
      void closeAndRelease(client, timeouts.closeMs);
      throw new Error("wake_database_acquire_abandoned");
    }
    return client;
  });
  try {
    return await withTimeout(
      pending,
      timeouts.acquireMs,
      "wake_database_acquire_timeout",
    );
  } catch {
    abandoned = true;
    if (acquired) await closeAndRelease(acquired, timeouts.closeMs);
    throw new Error("wake_database_acquire_failed");
  }
}

async function closeAndRelease(
  client: PoolClient,
  timeoutMs: number,
): Promise<void> {
  try {
    await withTimeout(client.end(), timeoutMs, "wake_database_close_timeout");
  } catch {
    // A failed or timed-out connection is never returned as a healthy pool client.
  } finally {
    client.release();
  }
}

async function withTimeout<T>(
  operation: Promise<T>,
  timeoutMs: number,
  code: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      operation,
      new Promise<T>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(code)), timeoutMs);
      }),
    ]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

export function createLazyPostgresWakePool(connectionString: string): Pool {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    throw new Error("wake_database_url_invalid");
  }
  const sslModes = url.searchParams.getAll("sslmode");
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    sslModes.length !== 1 ||
    !["require", "verify-ca", "verify-full"].includes(sslModes[0] ?? "") ||
    !url.hostname || !url.username || !url.password ||
    url.pathname.length < 2 ||
    url.searchParams.size !== 1 || url.hash
  ) {
    throw new Error("wake_database_tls_or_url_invalid");
  }

  let user: string;
  let password: string;
  let database: string;
  try {
    user = decodeURIComponent(url.username);
    password = decodeURIComponent(url.password);
    database = decodeURIComponent(url.pathname.slice(1));
  } catch {
    throw new Error("wake_database_url_invalid");
  }
  if (!database || database.includes("/")) {
    throw new Error("wake_database_url_invalid");
  }

  const port = url.port ? Number(url.port) : 5432;
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("wake_database_url_invalid");
  }
  try {
    return new Pool(
      {
        applicationName: "continumm-wake-dispatch",
        connection: { attempts: 1, interval: 500 },
        database,
        hostname: url.hostname,
        password,
        port,
        tls: { enabled: true, enforce: true, caCertificates: [] },
        user,
      },
      1,
      true,
    );
  } catch {
    throw new Error("wake_database_pool_invalid");
  }
}
