package com.workshop.flink.common.util;

/**
 * Deterministic mapping from accountId → currency. Keeps the joins demos free of an
 * extra lookup step when only the currency (not the full account row) is needed.
 *
 * Mapping is `hash(accountId) mod 4` → {USD, EUR, JPY, GBP}, so the same account
 * always maps to the same currency for the life of the workshop.
 */
public final class CurrencyOf {

    private static final String[] CURRENCIES = {"USD", "EUR", "JPY", "GBP"};

    public static String fromAccountId(String accountId) {
        int bucket = Math.floorMod(accountId.hashCode(), CURRENCIES.length);
        return CURRENCIES[bucket];
    }

    public static String[] all() {
        return CURRENCIES.clone();
    }

    private CurrencyOf() {}
}
