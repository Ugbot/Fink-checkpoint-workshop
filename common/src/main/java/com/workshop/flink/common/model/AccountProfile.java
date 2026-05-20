package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Wide account-profile row (~30 columns). Lives as a Fluss primary-key table
 * keyed by {@code accountId}. Used by lookup-join demos and the Java client
 * point-in-time lookup scenario.
 */
public class AccountProfile implements Serializable {

    private static final long serialVersionUID = 1L;

    // ── Identity ─────────────────────────────────────────────────────────────
    private String accountId;        // primary key (matches existing ACC-NNNN pattern)
    private String customerId;
    private String accountName;
    private String accountType;      // INDIVIDUAL / JOINT / CORPORATE / TRUST
    private String currency;

    // ── Segmentation ─────────────────────────────────────────────────────────
    private String tier;             // PLATINUM / GOLD / SILVER / BRONZE
    private String region;           // NA / EMEA / APAC / LATAM
    private String country;
    private String mifidClassification;   // RETAIL / PROFESSIONAL / ELIGIBLE_COUNTERPARTY
    private String fatcaStatus;
    private String taxResidency;

    // ── Compliance / risk ───────────────────────────────────────────────────
    private String kycStatus;        // PENDING / VERIFIED / EXPIRED / REJECTED
    private long   kycVerifiedAt;
    private String amlStatus;        // CLEAR / FLAGGED / UNDER_REVIEW
    private int    riskScore;        // 0..100
    private boolean pepFlag;         // politically exposed person
    private boolean sanctionsFlag;

    // ── Limits ──────────────────────────────────────────────────────────────
    private double dailyTradeLimit;
    private double monthlyTradeLimit;
    private double creditLine;
    private double availableBalance;
    private double marginUsed;

    // ── Banking ─────────────────────────────────────────────────────────────
    private String iban;
    private String swiftBic;
    private String custodianBank;

    // ── Lifecycle ───────────────────────────────────────────────────────────
    private long   openedAt;
    private long   lastLoginAt;
    private long   updatedAt;
    private boolean activeFlag;
    private String sourceSystem;

    public AccountProfile() {}

    public String getAccountId() { return accountId; }
    public void setAccountId(String v) { this.accountId = v; }
    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }
    public String getAccountName() { return accountName; }
    public void setAccountName(String v) { this.accountName = v; }
    public String getAccountType() { return accountType; }
    public void setAccountType(String v) { this.accountType = v; }
    public String getCurrency() { return currency; }
    public void setCurrency(String v) { this.currency = v; }
    public String getTier() { return tier; }
    public void setTier(String v) { this.tier = v; }
    public String getRegion() { return region; }
    public void setRegion(String v) { this.region = v; }
    public String getCountry() { return country; }
    public void setCountry(String v) { this.country = v; }
    public String getMifidClassification() { return mifidClassification; }
    public void setMifidClassification(String v) { this.mifidClassification = v; }
    public String getFatcaStatus() { return fatcaStatus; }
    public void setFatcaStatus(String v) { this.fatcaStatus = v; }
    public String getTaxResidency() { return taxResidency; }
    public void setTaxResidency(String v) { this.taxResidency = v; }
    public String getKycStatus() { return kycStatus; }
    public void setKycStatus(String v) { this.kycStatus = v; }
    public long getKycVerifiedAt() { return kycVerifiedAt; }
    public void setKycVerifiedAt(long v) { this.kycVerifiedAt = v; }
    public String getAmlStatus() { return amlStatus; }
    public void setAmlStatus(String v) { this.amlStatus = v; }
    public int getRiskScore() { return riskScore; }
    public void setRiskScore(int v) { this.riskScore = v; }
    public boolean isPepFlag() { return pepFlag; }
    public void setPepFlag(boolean v) { this.pepFlag = v; }
    public boolean isSanctionsFlag() { return sanctionsFlag; }
    public void setSanctionsFlag(boolean v) { this.sanctionsFlag = v; }
    public double getDailyTradeLimit() { return dailyTradeLimit; }
    public void setDailyTradeLimit(double v) { this.dailyTradeLimit = v; }
    public double getMonthlyTradeLimit() { return monthlyTradeLimit; }
    public void setMonthlyTradeLimit(double v) { this.monthlyTradeLimit = v; }
    public double getCreditLine() { return creditLine; }
    public void setCreditLine(double v) { this.creditLine = v; }
    public double getAvailableBalance() { return availableBalance; }
    public void setAvailableBalance(double v) { this.availableBalance = v; }
    public double getMarginUsed() { return marginUsed; }
    public void setMarginUsed(double v) { this.marginUsed = v; }
    public String getIban() { return iban; }
    public void setIban(String v) { this.iban = v; }
    public String getSwiftBic() { return swiftBic; }
    public void setSwiftBic(String v) { this.swiftBic = v; }
    public String getCustodianBank() { return custodianBank; }
    public void setCustodianBank(String v) { this.custodianBank = v; }
    public long getOpenedAt() { return openedAt; }
    public void setOpenedAt(long v) { this.openedAt = v; }
    public long getLastLoginAt() { return lastLoginAt; }
    public void setLastLoginAt(long v) { this.lastLoginAt = v; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long v) { this.updatedAt = v; }
    public boolean isActiveFlag() { return activeFlag; }
    public void setActiveFlag(boolean v) { this.activeFlag = v; }
    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String v) { this.sourceSystem = v; }

    @Override
    public String toString() {
        return "AccountProfile{accountId=" + accountId + ", tier=" + tier
                + ", region=" + region + ", riskScore=" + riskScore + "}";
    }
}
