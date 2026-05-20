-- Step 4 — Event-time tumbling window.
--
-- Lessons:
--   • Identical SELECT shape, but the window is now event-time driven.
--   • The window only emits its result when the watermark crosses the window's end.
--   • If the source pauses, the watermark stalls and windows do NOT emit even though
--     wall-clock time has passed. (Step 5 fixes this with idle-source timeout.)
--   • This output is DETERMINISTIC across replays — the same input always yields the
--     same groupings.

SELECT
  ticker,
  TUMBLE_START(trade_ts, INTERVAL '30' SECOND) AS window_start,
  TUMBLE_END  (trade_ts, INTERVAL '30' SECOND) AS window_end,
  COUNT(*)                                     AS trade_count
FROM kafka_trades_eventtime
GROUP BY ticker, TUMBLE(trade_ts, INTERVAL '30' SECOND);
