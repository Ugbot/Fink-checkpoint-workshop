package com.workshop.flink.scenario07.model;

import java.io.Serializable;

/**
 * Window-join output: one row per (ticker, window_start) with the count of
 * trades and quotes that arrived in that window.
 */
public class TickerVolumeRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ticker;
    private long   windowStart;
    private long   windowEnd;
    private long   tradeCount;
    private long   quoteCount;

    public TickerVolumeRow() {}

    public TickerVolumeRow(String ticker, long windowStart, long windowEnd,
                           long tradeCount, long quoteCount) {
        this.ticker      = ticker;
        this.windowStart = windowStart;
        this.windowEnd   = windowEnd;
        this.tradeCount  = tradeCount;
        this.quoteCount  = quoteCount;
    }

    public String getTicker()       { return ticker; }
    public long   getWindowStart()  { return windowStart; }
    public long   getWindowEnd()    { return windowEnd; }
    public long   getTradeCount()   { return tradeCount; }
    public long   getQuoteCount()   { return quoteCount; }

    public void setTicker(String ticker)             { this.ticker = ticker; }
    public void setWindowStart(long windowStart)     { this.windowStart = windowStart; }
    public void setWindowEnd(long windowEnd)         { this.windowEnd = windowEnd; }
    public void setTradeCount(long tradeCount)       { this.tradeCount = tradeCount; }
    public void setQuoteCount(long quoteCount)       { this.quoteCount = quoteCount; }

    @Override
    public String toString() {
        return String.format("TickerVolumeRow{ticker='%s', window=[%d,%d), trades=%d, quotes=%d}",
                             ticker, windowStart, windowEnd, tradeCount, quoteCount);
    }
}
