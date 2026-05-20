package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Partial-update source: the marketing / consent platform writes consent
 * + segmentation columns only.
 */
public class CustomerMarketing implements Serializable {

    private static final long serialVersionUID = 1L;

    private String customerId;        // primary key
    private boolean marketingOptIn;
    private boolean emailOptIn;
    private boolean smsOptIn;
    private String preferredChannel;  // EMAIL / SMS / PUSH / NONE
    private String segment;           // GROWTH / RETAIN / DORMANT / VIP
    private String acquisitionSource; // ORGANIC / PAID_SOCIAL / REFERRAL / PARTNER
    private long   lastEngagementAt;
    private long   updatedAt;

    public CustomerMarketing() {}

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String v) { this.customerId = v; }
    public boolean isMarketingOptIn() { return marketingOptIn; }
    public void setMarketingOptIn(boolean v) { this.marketingOptIn = v; }
    public boolean isEmailOptIn() { return emailOptIn; }
    public void setEmailOptIn(boolean v) { this.emailOptIn = v; }
    public boolean isSmsOptIn() { return smsOptIn; }
    public void setSmsOptIn(boolean v) { this.smsOptIn = v; }
    public String getPreferredChannel() { return preferredChannel; }
    public void setPreferredChannel(String v) { this.preferredChannel = v; }
    public String getSegment() { return segment; }
    public void setSegment(String v) { this.segment = v; }
    public String getAcquisitionSource() { return acquisitionSource; }
    public void setAcquisitionSource(String v) { this.acquisitionSource = v; }
    public long getLastEngagementAt() { return lastEngagementAt; }
    public void setLastEngagementAt(long v) { this.lastEngagementAt = v; }
    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long v) { this.updatedAt = v; }
}
