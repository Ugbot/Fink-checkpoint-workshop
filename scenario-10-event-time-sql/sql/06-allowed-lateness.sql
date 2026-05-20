-- Step 6 — Allow a grace period for late data to revise window output.
--
-- Lessons:
--   • The session property `table.exec.window-allowed-lateness` controls how long
--     a window's state is kept after the watermark first crossed its end.
--   • Each late event that arrives within this grace period RETRACTS and re-EMITS
--     the window result. Downstream consumers must handle retraction (Kafka Upsert,
--     Print, Paimon, etc.). Plain Kafka Append sinks reject retract streams.
--   • Set this to *zero* (default) and late events are silently dropped — what most
--     teams accidentally ship.
--
-- This file is meant to be pasted into the SQL Client so the SET takes effect for
-- the subsequent INSERT/SELECT. SET is session-scoped.

SET 'table.exec.window-allowed-lateness' = '1 min';

-- Run an event-time window with the relaxed lateness. Output will retract+emit
-- whenever a late record arrives within 1 minute of the window's watermark crossing.

SELECT
  ticker,
  TUMBLE_START(trade_ts, INTERVAL '30' SECOND) AS window_start,
  COUNT(*)                                     AS trade_count
FROM kafka_trades_eventtime
GROUP BY ticker, TUMBLE(trade_ts, INTERVAL '30' SECOND);
