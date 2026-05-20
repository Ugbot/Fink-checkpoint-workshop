package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * FX rate changelog event: rate of one unit of `currency` expressed in USD,
 * effective from `rateTime` until the next event for the same currency.
 *
 * The temporal-join demos use this as a versioned table: for a trade with
 * tradeTime = T, the matching FX rate is the one with the largest rateTime ≤ T.
 */
public class FxRate implements Serializable {

    private static final long serialVersionUID = 1L;

    private String currency;
    private double rate;
    private long   rateTime;

    public FxRate() {}

    public FxRate(String currency, double rate, long rateTime) {
        this.currency = currency;
        this.rate     = rate;
        this.rateTime = rateTime;
    }

    public String getCurrency() { return currency; }
    public double getRate()     { return rate; }
    public long   getRateTime() { return rateTime; }

    public void setCurrency(String currency) { this.currency = currency; }
    public void setRate(double rate)         { this.rate     = rate; }
    public void setRateTime(long rateTime)   { this.rateTime = rateTime; }

    @Override
    public String toString() {
        return String.format("FxRate{currency='%s', rate=%.6f, rateTime=%d}", currency, rate, rateTime);
    }
}
