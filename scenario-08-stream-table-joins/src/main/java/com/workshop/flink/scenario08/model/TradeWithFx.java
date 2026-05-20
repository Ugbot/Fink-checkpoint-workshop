package com.workshop.flink.scenario08.model;

import com.workshop.flink.common.model.TradeEvent;

import java.io.Serializable;

public class TradeWithFx implements Serializable {

    private static final long serialVersionUID = 1L;

    private TradeEvent trade;
    private String currency;
    private Double fxRate;          // null when no rate is yet known (LEFT-JOIN semantics)
    private Long   fxRateAsOf;      // rateTime of the applied rate, or null
    private double notionalUsd;     // quantity * price * fxRate (0 when fxRate is null)

    public TradeWithFx() {}

    public TradeWithFx(TradeEvent trade, String currency, Double fxRate, Long fxRateAsOf) {
        this.trade       = trade;
        this.currency    = currency;
        this.fxRate      = fxRate;
        this.fxRateAsOf  = fxRateAsOf;
        this.notionalUsd = fxRate == null ? 0.0 : trade.getQuantity() * trade.getPrice() * fxRate;
    }

    public TradeEvent getTrade()       { return trade; }
    public String     getCurrency()    { return currency; }
    public Double     getFxRate()      { return fxRate; }
    public Long       getFxRateAsOf()  { return fxRateAsOf; }
    public double     getNotionalUsd() { return notionalUsd; }

    public void setTrade(TradeEvent trade)           { this.trade = trade; }
    public void setCurrency(String currency)         { this.currency = currency; }
    public void setFxRate(Double fxRate)             { this.fxRate = fxRate; }
    public void setFxRateAsOf(Long fxRateAsOf)       { this.fxRateAsOf = fxRateAsOf; }
    public void setNotionalUsd(double notionalUsd)   { this.notionalUsd = notionalUsd; }

    @Override
    public String toString() {
        return String.format("TradeWithFx{trade=%s, currency='%s', fxRate=%s, asOf=%s, notionalUsd=%.2f}",
                             trade, currency, fxRate, fxRateAsOf, notionalUsd);
    }
}
