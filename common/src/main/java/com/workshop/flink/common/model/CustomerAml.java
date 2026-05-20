package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Partial-update source: the AML / sanctions screening subsystem. Writes only
 * AML-related columns into {@code customer_360}, never overwriting fields
 * owned by other sources.
 */
public class CustomerAml implements Serializable {

    private static final long serialVersionUID = 1L;

    private String customerId;        // primary key
    private String amlStatus;         // CLEAR / FLAGGED / UNDER_REVIEW
    private int    riskScore;         // 0..100, set by the AML engine
    private boolean pepFlag;          // politically exposed person
    private boolean sanctionsFlag;
    private String amlReviewedBy;
    private long   amlReviewedAt;
    private long   updatedAt;

    public CustomerAml() {}

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }
    public String getAmlStatus() { return amlStatus; }
    public void setAmlStatus(String v) { this.amlStatus = v; }
    public int getRiskScore() { return riskScore; }
    public void setRiskScore(int v) { this.riskScore = v; }
    public boolean isPepFlag() { return pepFlag; }
    public void setPepFlag(boolean v) { this.pepFlag = v; }
    public boolean isSanctionsFlag() { return sanctionsFlag; }
    public void setSanctionsFlag(boolean v) { this.sanctionsFlag = v; }
    public String getAmlReviewedBy() { return amlReviewedBy; }
    public void setAmlReviewedBy(String v) { this.amlReviewedBy = v; }
    public long getAmlReviewedAt() { return amlReviewedAt; }
    public void setAmlReviewedAt(long v) { this.amlReviewedAt = v; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long v) { this.updatedAt = v; }
}
