package com.workshop.flink.common.util;

import com.workshop.flink.common.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class CrashTrigger implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(CrashTrigger.class);

    private final int crashAfter;

    // Transient: NOT written to Flink checkpoint state. Resets to 0 on every task restart.
    // This is intentional — the crash re-fires on the replayed records when crashAfter is
    // smaller than the number of records processed since the last checkpoint.
    // For the workshop demos, set crashAfter large enough that the crash fires before the
    // next checkpoint commits, then never fires again after restart (since count resets).
    private transient int count;

    public CrashTrigger(int crashAfter) {
        this.crashAfter = crashAfter;
    }

    public void increment() {
        if (++count >= crashAfter) {
            LOG.warn("CrashTrigger: firing intentional crash after {} records", count);
            throw new RuntimeException(
                "CrashTrigger: intentional crash after " + count + " records " +
                "(configured limit: " + crashAfter + ")");
        }
    }

    public static CrashTrigger fromEnv() {
        String val = System.getenv(Constants.CRASH_AFTER_RECORDS_ENV);
        if (val == null || val.isBlank()) {
            return disabled();
        }
        int limit = Integer.parseInt(val.trim());
        LOG.info("CrashTrigger: will crash after {} records", limit);
        return new CrashTrigger(limit);
    }

    public static CrashTrigger disabled() {
        return new CrashTrigger(Integer.MAX_VALUE);
    }
}
