package com.workshop.flink.common.model;

import java.io.Serializable;
import java.util.UUID;

public class QuoteEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String quoteId;
    private String ticker;
    private double bid;
    private double ask;
    private long   quoteTime;

    public QuoteEvent() {}

    private QuoteEvent(String quoteId, String ticker, double bid, double ask, long quoteTime) {
        this.quoteId   = quoteId;
        this.ticker    = ticker;
        this.bid       = bid;
        this.ask       = ask;
        this.quoteTime = quoteTime;
    }

    public static QuoteEvent of(String ticker, double bid, double ask) {
        return new QuoteEvent(UUID.randomUUID().toString(), ticker, bid, ask, System.currentTimeMillis());
    }

    public String getQuoteId()  { return quoteId; }
    public String getTicker()   { return ticker; }
    public double getBid()      { return bid; }
    public double getAsk()      { return ask; }
    public long   getQuoteTime() { return quoteTime; }

    public void setQuoteId(String quoteId)    { this.quoteId   = quoteId; }
    public void setTicker(String ticker)      { this.ticker    = ticker; }
    public void setBid(double bid)            { this.bid       = bid; }
    public void setAsk(double ask)            { this.ask       = ask; }
    public void setQuoteTime(long quoteTime)  { this.quoteTime = quoteTime; }

    public double getMid() { return (bid + ask) / 2.0; }

    @Override
    public String toString() {
        return String.format("QuoteEvent{quoteId='%s', ticker='%s', bid=%.4f, ask=%.4f, quoteTime=%d}",
                             quoteId, ticker, bid, ask, quoteTime);
    }
}
