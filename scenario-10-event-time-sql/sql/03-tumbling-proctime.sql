-- Step 3 — Processing-time tumbling window.
--
-- Lessons:
--   • Windows on processing-time fire on wall-clock boundaries — operator-local time.
--   • Restart the job and the window boundaries shift. Same input, different output.
--   • Use only when you don't care about correctness under replay (e.g., live dashboards).

-- Run against the table declared in step 1.
SELECT
  ticker,
  TUMBLE_START(proc_ts, INTERVAL '30' SECOND) AS window_start,
  TUMBLE_END  (proc_ts, INTERVAL '30' SECOND) AS window_end,
  COUNT(*)                                    AS trade_count
FROM kafka_trades_proctime
GROUP BY ticker, TUMBLE(proc_ts, INTERVAL '30' SECOND);
