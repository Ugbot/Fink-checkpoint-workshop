package com.workshop.flink.scenario07.model;

import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;

import java.io.Serializable;

/**
 * Output of the interval-join in scenario 07b: trade tagged with the prevailing
 * quote for the same ticker within ±5s of the trade time.
 */
public class TradeWithQuote implements Serializable {

    private static final long serialVersionUID = 1L;

    private TradeEvent trade;
    private QuoteEvent quote;
    private long lagMs;   // trade.tradeTime - quote.quoteTime (signed)

    public TradeWithQuote() {}

    public TradeWithQuote(TradeEvent trade, QuoteEvent quote) {
        this.trade = trade;
        this.quote = quote;
        this.lagMs = trade.getTradeTime() - quote.getQuoteTime();
    }

    public TradeEvent getTrade() { return trade; }
    public QuoteEvent getQuote() { return quote; }
    public long getLagMs()       { return lagMs; }

    public void setTrade(TradeEvent trade) { this.trade = trade; }
    public void setQuote(QuoteEvent quote) { this.quote = quote; }
    public void setLagMs(long lagMs)       { this.lagMs = lagMs; }

    @Override
    public String toString() {
        return String.format("TradeWithQuote{trade=%s, quote=%s, lagMs=%d}", trade, quote, lagMs);
    }
}
