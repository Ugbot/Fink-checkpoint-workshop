package com.workshop.flink.common.model;

import java.io.Serializable;
import java.util.UUID;

/**
 * Append-only wide trade row used by the Fluss {@code trade_log_wide} log table
 * (scenarios 12, 16). Captures both the raw trade and several enrichment
 * columns that would normally arrive via downstream joins — they're populated
 * by the datagen for self-contained workshop demos.
 *
 * <p>The width matters: scenario-12 step 03 demonstrates projection pushdown
 * by selecting only 3 of these 25 columns and inspecting the EXPLAIN plan.
 */
public class TradeWide implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;       // unique per trade
    private String accountId;
    private String customerId;
    private String ticker;
    private String isin;
    private String side;          // BUY / SELL
    private String orderType;     // MARKET / LIMIT / STOP / STOP_LIMIT
    private int    quantity;
    private double price;
    private double notional;      // quantity * price
    private String currency;
    private double fxRateToUsd;
    private double notionalUsd;
    private String exchange;
    private String venue;         // exchange-internal sub-venue
    private String sector;
    private String country;
    private String desk;          // trading desk
    private String trader;        // trader id
    private long   tradeTime;     // epoch millis
    private long   settledAt;
    private String settlementStatus;   // PENDING / SETTLED / FAILED
    private double commission;
    private double tax;
    private String sourceApp;

    public TradeWide() {}

    public static TradeWide of(String accountId, String ticker, String side, int qty, double price) {
        TradeWide t = new TradeWide();
        t.eventId  = UUID.randomUUID().toString();
        t.accountId = accountId;
        t.ticker    = ticker;
        t.side      = side;
        t.quantity  = qty;
        t.price     = price;
        t.notional  = qty * price;
        t.tradeTime = System.currentTimeMillis();
        return t;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String v) { this.eventId = v; }
    public String getAccountId() { return accountId; }
    public void setAccountId(String v) { this.accountId = v; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }
    public String getTicker() { return ticker; }
    public void setTicker(String v) { this.ticker = v; }
    public String getIsin() { return isin; }
    public void setIsin(String v) { this.isin = v; }
    public String getSide() { return side; }
    public void setSide(String v) { this.side = v; }
    public String getOrderType() { return orderType; }
    public void setOrderType(String v) { this.orderType = v; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int v) { this.quantity = v; }
    public double getPrice() { return price; }
    public void setPrice(double v) { this.price = v; }
    public double getNotional() { return notional; }
    public void setNotional(double v) { this.notional = v; }
    public String getCurrency() { return currency; }
    public void setCurrency(String v) { this.currency = v; }
    public double getFxRateToUsd() { return fxRateToUsd; }
    public void setFxRateToUsd(double v) { this.fxRateToUsd = v; }
    public double getNotionalUsd() { return notionalUsd; }
    public void setNotionalUsd(double v) { this.notionalUsd = v; }
    public String getExchange() { return exchange; }
    public void setExchange(String v) { this.exchange = v; }
    public String getVenue() { return venue; }
    public void setVenue(String v) { this.venue = v; }
    public String getSector() { return sector; }
    public void setSector(String v) { this.sector = v; }
    public String getCountry() { return country; }
    public void setCountry(String v) { this.country = v; }
    public String getDesk() { return desk; }
    public void setDesk(String v) { this.desk = v; }
    public String getTrader() { return trader; }
    public void setTrader(String v) { this.trader = v; }
    public long getTradeTime() { return tradeTime; }
    public void setTradeTime(long v) { this.tradeTime = v; }
    public long getSettledAt() { return settledAt; }
    public void setSettledAt(long v) { this.settledAt = v; }
    public String getSettlementStatus() { return settlementStatus; }
    public void setSettlementStatus(String v) { this.settlementStatus = v; }
    public double getCommission() { return commission; }
    public void setCommission(double v) { this.commission = v; }
    public double getTax() { return tax; }
    public void setTax(double v) { this.tax = v; }
    public String getSourceApp() { return sourceApp; }
    public void setSourceApp(String v) { this.sourceApp = v; }
}
