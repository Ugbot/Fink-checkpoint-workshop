package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Wide security-master row (~45 columns). Lives as a Fluss primary-key table
 * keyed by {@code isin}. Designed so projection-pushdown demos are dramatic —
 * a query selecting 3 columns out of 45 should hit storage for only those 3.
 *
 * <p>POJO with public no-arg constructor + getters/setters per Flink's
 * {@code POJOSerializer} requirements.
 */
public class InstrumentMaster implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Identity & venue ─────────────────────────────────────────────────────
    private String isin;          // primary key
    private String cusip;
    private String sedol;
    private String ticker;
    private String exchange;      // e.g. NASDAQ, NYSE, LSE
    private String mic;           // ISO 10383 market identifier code

    // ── Classification ───────────────────────────────────────────────────────
    private String securityType;  // EQUITY / BOND / ETF / OPTION / FX
    private String sector;        // GICS sector
    private String subSector;     // GICS sub-sector / industry
    private String country;       // ISO 3166-1 alpha-2
    private String currency;      // ISO 4217

    // ── Trading characteristics ──────────────────────────────────────────────
    private double lotSize;
    private double tickSize;
    private double minNotional;
    private double maxOrderQty;
    private boolean shortable;
    private boolean marginable;

    // ── Reference prices ─────────────────────────────────────────────────────
    private double lastClose;
    private double fiftyTwoWeekHigh;
    private double fiftyTwoWeekLow;
    private double prevDayVolume;
    private double avgDailyVolume30d;
    private double avgDailyVolume90d;

    // ── Risk / hedge ─────────────────────────────────────────────────────────
    private double beta1y;
    private double beta3y;
    private double beta5y;
    private double sectorBeta;
    private double hedgeRatio;
    private double volatility30d;
    private double valueAtRisk95;

    // ── Fundamentals ─────────────────────────────────────────────────────────
    private double marketCapUsd;
    private double freeFloat;
    private double dividendYield;
    private double priceEarnings;
    private double priceBook;
    private double debtToEquity;

    // ── ESG ──────────────────────────────────────────────────────────────────
    private double esgScore;
    private double environmentalScore;
    private double socialScore;
    private double governanceScore;

    // ── Lifecycle ────────────────────────────────────────────────────────────
    private long   listedAt;       // epoch millis
    private long   updatedAt;
    private String issuerName;
    private String issuerLei;      // legal entity identifier
    private boolean activeFlag;
    private String regulatoryStatus;
    private String sourceSystem;

    public InstrumentMaster() {}

    // ── Getters / setters ────────────────────────────────────────────────────
    public String getIsin() { return isin; }
    public void setIsin(String v) { this.isin = v; }
    public String getCusip() { return cusip; }
    public void setCusip(String v) { this.cusip = v; }
    public String getSedol() { return sedol; }
    public void setSedol(String v) { this.sedol = v; }
    public String getTicker() { return ticker; }
    public void setTicker(String v) { this.ticker = v; }
    public String getExchange() { return exchange; }
    public void setExchange(String v) { this.exchange = v; }
    public String getMic() { return mic; }
    public void setMic(String v) { this.mic = v; }
    public String getSecurityType() { return securityType; }
    public void setSecurityType(String v) { this.securityType = v; }
    public String getSector() { return sector; }
    public void setSector(String v) { this.sector = v; }
    public String getSubSector() { return subSector; }
    public void setSubSector(String v) { this.subSector = v; }
    public String getCountry() { return country; }
    public void setCountry(String v) { this.country = v; }
    public String getCurrency() { return currency; }
    public void setCurrency(String v) { this.currency = v; }
    public double getLotSize() { return lotSize; }
    public void setLotSize(double v) { this.lotSize = v; }
    public double getTickSize() { return tickSize; }
    public void setTickSize(double v) { this.tickSize = v; }
    public double getMinNotional() { return minNotional; }
    public void setMinNotional(double v) { this.minNotional = v; }
    public double getMaxOrderQty() { return maxOrderQty; }
    public void setMaxOrderQty(double v) { this.maxOrderQty = v; }
    public boolean isShortable() { return shortable; }
    public void setShortable(boolean v) { this.shortable = v; }
    public boolean isMarginable() { return marginable; }
    public void setMarginable(boolean v) { this.marginable = v; }
    public double getLastClose() { return lastClose; }
    public void setLastClose(double v) { this.lastClose = v; }
    public double getFiftyTwoWeekHigh() { return fiftyTwoWeekHigh; }
    public void setFiftyTwoWeekHigh(double v) { this.fiftyTwoWeekHigh = v; }
    public double getFiftyTwoWeekLow() { return fiftyTwoWeekLow; }
    public void setFiftyTwoWeekLow(double v) { this.fiftyTwoWeekLow = v; }
    public double getPrevDayVolume() { return prevDayVolume; }
    public void setPrevDayVolume(double v) { this.prevDayVolume = v; }
    public double getAvgDailyVolume30d() { return avgDailyVolume30d; }
    public void setAvgDailyVolume30d(double v) { this.avgDailyVolume30d = v; }
    public double getAvgDailyVolume90d() { return avgDailyVolume90d; }
    public void setAvgDailyVolume90d(double v) { this.avgDailyVolume90d = v; }
    public double getBeta1y() { return beta1y; }
    public void setBeta1y(double v) { this.beta1y = v; }
    public double getBeta3y() { return beta3y; }
    public void setBeta3y(double v) { this.beta3y = v; }
    public double getBeta5y() { return beta5y; }
    public void setBeta5y(double v) { this.beta5y = v; }
    public double getSectorBeta() { return sectorBeta; }
    public void setSectorBeta(double v) { this.sectorBeta = v; }
    public double getHedgeRatio() { return hedgeRatio; }
    public void setHedgeRatio(double v) { this.hedgeRatio = v; }
    public double getVolatility30d() { return volatility30d; }
    public void setVolatility30d(double v) { this.volatility30d = v; }
    public double getValueAtRisk95() { return valueAtRisk95; }
    public void setValueAtRisk95(double v) { this.valueAtRisk95 = v; }
    public double getMarketCapUsd() { return marketCapUsd; }
    public void setMarketCapUsd(double v) { this.marketCapUsd = v; }
    public double getFreeFloat() { return freeFloat; }
    public void setFreeFloat(double v) { this.freeFloat = v; }
    public double getDividendYield() { return dividendYield; }
    public void setDividendYield(double v) { this.dividendYield = v; }
    public double getPriceEarnings() { return priceEarnings; }
    public void setPriceEarnings(double v) { this.priceEarnings = v; }
    public double getPriceBook() { return priceBook; }
    public void setPriceBook(double v) { this.priceBook = v; }
    public double getDebtToEquity() { return debtToEquity; }
    public void setDebtToEquity(double v) { this.debtToEquity = v; }
    public double getEsgScore() { return esgScore; }
    public void setEsgScore(double v) { this.esgScore = v; }
    public double getEnvironmentalScore() { return environmentalScore; }
    public void setEnvironmentalScore(double v) { this.environmentalScore = v; }
    public double getSocialScore() { return socialScore; }
    public void setSocialScore(double v) { this.socialScore = v; }
    public double getGovernanceScore() { return governanceScore; }
    public void setGovernanceScore(double v) { this.governanceScore = v; }
    public long getListedAt() { return listedAt; }
    public void setListedAt(long v) { this.listedAt = v; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long v) { this.updatedAt = v; }
    public String getIssuerName() { return issuerName; }
    public void setIssuerName(String v) { this.issuerName = v; }
    public String getIssuerLei() { return issuerLei; }
    public void setIssuerLei(String v) { this.issuerLei = v; }
    public boolean isActiveFlag() { return activeFlag; }
    public void setActiveFlag(boolean v) { this.activeFlag = v; }
    public String getRegulatoryStatus() { return regulatoryStatus; }
    public void setRegulatoryStatus(String v) { this.regulatoryStatus = v; }
    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String v) { this.sourceSystem = v; }

    @Override
    public String toString() {
        return "InstrumentMaster{isin=" + isin + ", ticker=" + ticker
                + ", sector=" + sector + ", country=" + country + "}";
    }
}
