import { Pool } from "@db/postgres";
import type {
  ClaimOutcome,
  Delivery,
  RecoveryState,
  RecoveryStore,
} from "./handler.ts";

export function createPostgresRecoveryStore(pool: Pool): RecoveryStore {
  return {
    async consumeTicket(ticketId, token) {
      const client = await pool.connect();
      try {
        const result = await client.queryObject<{ valid: boolean }>`
          SELECT recovery_private.consume_scan_ticket(${ticketId}::uuid, ${token}) AS valid
        `;
        return result.rows[0]?.valid === true;
      } finally {
        client.release();
      }
    },
    async beginScan() {
      const client = await pool.connect();
      try {
        await client.queryArray("BEGIN");
        const result = await client.queryObject<{
          watermark_at: Date | null;
          scan_cursor: string | null;
          pending_watermark_at: Date | null;
          last_scan_at: Date | null;
          history_gap_since: Date | null;
        }>`
          UPDATE recovery_private.scan_state
          SET lease_until = clock_timestamp() + interval '4 minutes'
          WHERE singleton = true
            AND (lease_until IS NULL OR lease_until <= clock_timestamp())
          RETURNING watermark_at, scan_cursor, pending_watermark_at,
                    last_scan_at, history_gap_since
        `;
        await client.queryArray("COMMIT");
        const row = result.rows[0];
        if (!row) return null;
        return {
          watermark: row.watermark_at?.toISOString() ?? null,
          scanCursor: row.scan_cursor,
          pendingWatermark: row.pending_watermark_at?.toISOString() ?? null,
          lastScanAt: row.last_scan_at?.toISOString() ?? null,
          historyGapSince: row.history_gap_since?.toISOString() ?? null,
        } satisfies RecoveryState;
      } catch (error) {
        await client.queryArray("ROLLBACK").catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
    },
    async saveScanProgress(cursor, pendingWatermark) {
      const client = await pool.connect();
      try {
        await client.queryObject`
          UPDATE recovery_private.scan_state
          SET scan_cursor = ${cursor},
              pending_watermark_at = CASE
                WHEN ${pendingWatermark}::timestamptz IS NULL THEN pending_watermark_at
                ELSE GREATEST(pending_watermark_at, ${pendingWatermark}::timestamptz)
              END
          WHERE singleton = true
        `;
      } finally {
        client.release();
      }
    },
    async claimDelivery(delivery: Delivery): Promise<ClaimOutcome> {
      const client = await pool.connect();
      try {
        const inserted = await client.queryObject<{ delivery_guid: string }>`
          INSERT INTO recovery_private.delivery_redelivery
            (delivery_guid, delivery_id, delivered_at)
          VALUES (${delivery.guid}::uuid, ${delivery.id}::bigint,
                  ${delivery.delivered_at}::timestamptz)
          ON CONFLICT (delivery_guid) DO NOTHING
          RETURNING delivery_guid
        `;
        if (inserted.rows.length > 0) return "claimed";
        const retried = await client.queryObject<{ delivery_guid: string }>`
          UPDATE recovery_private.delivery_redelivery
          SET outcome = 'claimed', delivery_id = ${delivery.id}::bigint,
              delivered_at = ${delivery.delivered_at}::timestamptz,
              retry_after = NULL, http_status = NULL, claimed_at = clock_timestamp()
          WHERE delivery_guid = ${delivery.guid}::uuid
            AND outcome = 'rate_limited'
            AND retry_after IS NOT NULL
            AND retry_after <= clock_timestamp()
          RETURNING delivery_guid
        `;
        return retried.rows.length > 0 ? "claimed" : "already_requested";
      } finally {
        client.release();
      }
    },
    async recordDeliveryOutcome(guid, outcome, status, retryAt) {
      const client = await pool.connect();
      try {
        await client.queryObject`
          UPDATE recovery_private.delivery_redelivery
          SET outcome = ${outcome}, http_status = ${status},
              retry_after = ${retryAt}::timestamptz
          WHERE delivery_guid = ${guid}::uuid AND outcome = 'claimed'
        `;
      } finally {
        client.release();
      }
    },
    async completeScan(input) {
      const client = await pool.connect();
      try {
        await client.queryObject`
          UPDATE recovery_private.scan_state
          SET watermark_at = CASE
                WHEN ${input.watermark}::timestamptz IS NULL THEN watermark_at
                ELSE GREATEST(watermark_at, ${input.watermark}::timestamptz)
              END,
              last_scan_at = CASE WHEN ${input.successful} THEN ${input.scannedAt}::timestamptz
                                  ELSE last_scan_at END,
              scan_cursor = CASE WHEN ${input.successful} THEN NULL ELSE scan_cursor END,
              pending_watermark_at = CASE WHEN ${input.successful} THEN NULL
                                          ELSE pending_watermark_at END,
              history_gap_since = COALESCE(history_gap_since,
                  ${input.historyGapSince}::timestamptz),
              lease_until = NULL
          WHERE singleton = true
        `;
      } finally {
        client.release();
      }
    },
  };
}

export function createLazyRecoveryPool(connectionString: string): Pool {
  let url: URL;
  try {
    url = new URL(connectionString);
  } catch {
    throw new Error("recovery_database_url_invalid");
  }
  if (
    (url.protocol !== "postgres:" && url.protocol !== "postgresql:") ||
    !url.hostname || !url.username || !url.password ||
    url.pathname.length < 2 ||
    url.searchParams.get("sslmode") !== "require" ||
    url.searchParams.size !== 1 || url.hash
  ) throw new Error("recovery_database_url_invalid");
  let user: string;
  let password: string;
  let database: string;
  try {
    user = decodeURIComponent(url.username);
    password = decodeURIComponent(url.password);
    database = decodeURIComponent(url.pathname.slice(1));
  } catch {
    throw new Error("recovery_database_url_invalid");
  }
  return new Pool(
    {
      applicationName: "continumm-github-delivery-recovery",
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
}
