package com.workshop.flink.common.model;

import java.io.Serializable;

/**
 * Slow-changing dimension row for the `accounts` Postgres table.
 * Used by the lookup-join demos as the enrichment target.
 */
public class AccountInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private String accountId;
    private String accountName;
    private String tier;       // PLATINUM | GOLD | SILVER | BRONZE
    private String region;     // NA | EMEA | APAC | LATAM

    public AccountInfo() {}

    public AccountInfo(String accountId, String accountName, String tier, String region) {
        this.accountId   = accountId;
        this.accountName = accountName;
        this.tier        = tier;
        this.region      = region;
    }

    public String getAccountId()   { return accountId; }
    public String getAccountName() { return accountName; }
    public String getTier()        { return tier; }
    public String getRegion()      { return region; }

    public void setAccountId(String accountId)     { this.accountId   = accountId; }
    public void setAccountName(String accountName) { this.accountName = accountName; }
    public void setTier(String tier)               { this.tier        = tier; }
    public void setRegion(String region)           { this.region      = region; }

    @Override
    public String toString() {
        return String.format("AccountInfo{accountId='%s', accountName='%s', tier='%s', region='%s'}",
                             accountId, accountName, tier, region);
    }
}
