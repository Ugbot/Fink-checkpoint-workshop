package com.workshop.flink.scenario07.model;

import com.workshop.flink.common.model.TradeEvent;

import java.io.Serializable;

/**
 * Output row of the regular join in scenario 07a. A single output type carries all four
 * join kinds — INNER / LEFT_ORPHAN / RIGHT_ORPHAN / FULL — so we can teach all four
 * variants from one job.
 *
 * INNER         — both order and fill present
 * LEFT_ORPHAN   — order present, no fill arrived within the TTL window (LEFT OUTER side)
 * RIGHT_ORPHAN  — fill present, no order ever matched (RIGHT OUTER side)
 * FULL          — synonym view: union of LEFT_ORPHAN and RIGHT_ORPHAN with INNER
 */
public class JoinedOrderFill implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum JoinKind { INNER, LEFT_ORPHAN, RIGHT_ORPHAN }

    private String   eventId;     // shared key (orderId / fillId)
    private JoinKind joinKind;
    private TradeEvent order;     // may be null when joinKind == RIGHT_ORPHAN
    private TradeEvent fill;      // may be null when joinKind == LEFT_ORPHAN
    private long emittedAt;

    public JoinedOrderFill() {}

    public JoinedOrderFill(String eventId, JoinKind joinKind, TradeEvent order, TradeEvent fill) {
        this.eventId  = eventId;
        this.joinKind = joinKind;
        this.order    = order;
        this.fill     = fill;
        this.emittedAt = System.currentTimeMillis();
    }

    public String getEventId()        { return eventId; }
    public JoinKind getJoinKind()     { return joinKind; }
    public TradeEvent getOrder()      { return order; }
    public TradeEvent getFill()       { return fill; }
    public long getEmittedAt()        { return emittedAt; }

    public void setEventId(String eventId)         { this.eventId   = eventId; }
    public void setJoinKind(JoinKind joinKind)     { this.joinKind  = joinKind; }
    public void setOrder(TradeEvent order)         { this.order     = order; }
    public void setFill(TradeEvent fill)           { this.fill      = fill; }
    public void setEmittedAt(long emittedAt)       { this.emittedAt = emittedAt; }

    @Override
    public String toString() {
        return String.format("JoinedOrderFill{eventId='%s', joinKind=%s, order=%s, fill=%s}",
                             eventId, joinKind, order, fill);
    }
}
