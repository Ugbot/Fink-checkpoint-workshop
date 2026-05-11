package com.workshop.flink.common.model;

import java.io.Serializable;
import java.util.UUID;

public class TradeEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;    // UUID string — dedup key across all scenarios
    private String accountId;  // e.g. "ACC-0042"
    private String ticker;     // e.g. "AAPL", "GS"
    private String side;       // "BUY" or "SELL"
    private int    quantity;   // number of shares
    private double price;      // per-share price in USD
    private long   tradeTime;  // epoch millis
    private String sourceApp;  // set by enrichment operators, e.g. "app1-s01"

    // No-arg constructor required for Flink POJO serializer and Jackson
    public TradeEvent() {}

    private TradeEvent(String eventId, String accountId, String ticker,
                       String side, int quantity, double price,
                       long tradeTime, String sourceApp) {
        this.eventId   = eventId;
        this.accountId = accountId;
        this.ticker    = ticker;
        this.side      = side;
        this.quantity  = quantity;
        this.price     = price;
        this.tradeTime = tradeTime;
        this.sourceApp = sourceApp;
    }

    public static TradeEvent of(String accountId, String ticker,
                                String side, int quantity, double price) {
        return new TradeEvent(
            UUID.randomUUID().toString(),
            accountId, ticker, side, quantity, price,
            System.currentTimeMillis(),
            "unknown"
        );
    }

    public String getEventId()   { return eventId; }
    public String getAccountId() { return accountId; }
    public String getTicker()    { return ticker; }
    public String getSide()      { return side; }
    public int    getQuantity()  { return quantity; }
    public double getPrice()     { return price; }
    public long   getTradeTime() { return tradeTime; }
    public String getSourceApp() { return sourceApp; }

    public void setEventId(String eventId)     { this.eventId   = eventId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }
    public void setTicker(String ticker)       { this.ticker    = ticker; }
    public void setSide(String side)           { this.side      = side; }
    public void setQuantity(int quantity)      { this.quantity  = quantity; }
    public void setPrice(double price)         { this.price     = price; }
    public void setTradeTime(long tradeTime)   { this.tradeTime = tradeTime; }
    public void setSourceApp(String sourceApp) { this.sourceApp = sourceApp; }

    @Override
    public String toString() {
        return String.format("TradeEvent{eventId='%s', accountId='%s', ticker='%s', side='%s', " +
                             "quantity=%d, price=%.2f, tradeTime=%d, sourceApp='%s'}",
                             eventId, accountId, ticker, side, quantity, price, tradeTime, sourceApp);
    }
}
