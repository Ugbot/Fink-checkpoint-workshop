package com.workshop.flink.scenario08.model;

import com.workshop.flink.common.model.AccountInfo;
import com.workshop.flink.common.model.TradeEvent;

import java.io.Serializable;

public class TradeWithAccount implements Serializable {

    private static final long serialVersionUID = 1L;

    private TradeEvent  trade;
    private AccountInfo account;   // null when no account row exists (LEFT-JOIN semantics)
    private boolean cacheHit;       // diagnostic: whether the lookup hit the Caffeine cache
    private long emittedAt;

    public TradeWithAccount() {}

    public TradeWithAccount(TradeEvent trade, AccountInfo account, boolean cacheHit) {
        this.trade     = trade;
        this.account   = account;
        this.cacheHit  = cacheHit;
        this.emittedAt = System.currentTimeMillis();
    }

    public TradeEvent  getTrade()    { return trade; }
    public AccountInfo getAccount()  { return account; }
    public boolean     isCacheHit()  { return cacheHit; }
    public long        getEmittedAt() { return emittedAt; }

    public void setTrade(TradeEvent trade)         { this.trade = trade; }
    public void setAccount(AccountInfo account)    { this.account = account; }
    public void setCacheHit(boolean cacheHit)      { this.cacheHit = cacheHit; }
    public void setEmittedAt(long emittedAt)       { this.emittedAt = emittedAt; }

    @Override
    public String toString() {
        return String.format("TradeWithAccount{trade=%s, account=%s, cacheHit=%s}",
                             trade, account, cacheHit);
    }
}
