package com.workshop.flink.scenario09.model;

import com.workshop.flink.common.model.AccountInfo;
import com.workshop.flink.common.model.QuoteEvent;
import com.workshop.flink.common.model.TradeEvent;

import java.io.Serializable;

/**
 * Final output of the multi-way pipeline: trade + prevailing quote + AS-OF FX rate +
 * account dimension. Carries enough fields for the verify script to assert correctness
 * across crash + recovery.
 */
public class EnrichedTrade implements Serializable {

    private static final long serialVersionUID = 1L;

    private TradeEvent  trade;
    private QuoteEvent  quote;
    private String      currency;
    private Double      fxRate;
    private Long        fxRateAsOf;
    private AccountInfo account;
    private double      notionalUsd;
    private long        emittedAt;

    public EnrichedTrade() {}

    public EnrichedTrade(TradeEvent trade, QuoteEvent quote,
                         String currency, Double fxRate, Long fxRateAsOf,
                         AccountInfo account) {
        this.trade      = trade;
        this.quote      = quote;
        this.currency   = currency;
        this.fxRate     = fxRate;
        this.fxRateAsOf = fxRateAsOf;
        this.account    = account;
        this.notionalUsd = (fxRate == null) ? 0.0
            : trade.getQuantity() * trade.getPrice() * fxRate;
        this.emittedAt = System.currentTimeMillis();
    }

    public TradeEvent  getTrade()       { return trade; }
    public QuoteEvent  getQuote()       { return quote; }
    public String      getCurrency()    { return currency; }
    public Double      getFxRate()      { return fxRate; }
    public Long        getFxRateAsOf()  { return fxRateAsOf; }
    public AccountInfo getAccount()     { return account; }
    public double      getNotionalUsd() { return notionalUsd; }
    public long        getEmittedAt()   { return emittedAt; }

    public void setTrade(TradeEvent trade)         { this.trade = trade; }
    public void setQuote(QuoteEvent quote)         { this.quote = quote; }
    public void setCurrency(String currency)       { this.currency = currency; }
    public void setFxRate(Double fxRate)           { this.fxRate = fxRate; }
    public void setFxRateAsOf(Long fxRateAsOf)     { this.fxRateAsOf = fxRateAsOf; }
    public void setAccount(AccountInfo account)    { this.account = account; }
    public void setNotionalUsd(double notionalUsd) { this.notionalUsd = notionalUsd; }
    public void setEmittedAt(long emittedAt)       { this.emittedAt = emittedAt; }

    @Override
    public String toString() {
        return String.format(
            "EnrichedTrade{eventId=%s, ticker=%s, qty=%d, price=%.2f, " +
            "quote=%s, currency=%s, fxRate=%s, account=%s, notionalUsd=%.2f}",
            trade == null ? null : trade.getEventId(),
            trade == null ? null : trade.getTicker(),
            trade == null ? 0 : trade.getQuantity(),
            trade == null ? 0.0 : trade.getPrice(),
            quote, currency, fxRate, account, notionalUsd);
    }
}
