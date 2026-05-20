package com.workshop.flink.common.datagen;

import com.workshop.flink.common.model.FxRate;
import com.workshop.flink.common.util.CurrencyOf;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Random-walk FX rate generator. Each tick picks one currency and nudges its rate
 * by ±0.2%. The first time we see a currency, we seed its base rate from a fixed
 * table so the walks stay realistic across job restarts.
 */
public class FxRateGeneratorFunction implements GeneratorFunction<Long, FxRate> {

    private static final long serialVersionUID = 1L;

    /** Baseline starting rates (currency → USD per 1 unit of currency). */
    private static final double[] BASE_RATES = baseRatesAligned();

    // The walker uses a small in-memory map keyed by currency. Generators are
    // instantiated per subtask, but this job runs at parallelism 1.
    private transient double[] current;

    private static double[] baseRatesAligned() {
        String[] all = CurrencyOf.all();
        double[] out = new double[all.length];
        for (int i = 0; i < all.length; i++) {
            switch (all[i]) {
                case "USD": out[i] = 1.0;     break;
                case "EUR": out[i] = 1.09;    break;
                case "JPY": out[i] = 0.0063;  break;
                case "GBP": out[i] = 1.27;    break;
                default:    out[i] = 1.0;
            }
        }
        return out;
    }

    @Override
    public FxRate map(Long index) {
        if (current == null) {
            current = BASE_RATES.clone();
        }
        String[] all = CurrencyOf.all();
        int i = (int) Math.floorMod(index, all.length);

        // ±0.2% random walk, clamped within ±20% of base
        double drift = ThreadLocalRandom.current().nextDouble(-0.002, 0.002);
        double next  = current[i] * (1.0 + drift);
        double lo    = BASE_RATES[i] * 0.8;
        double hi    = BASE_RATES[i] * 1.2;
        next = Math.max(lo, Math.min(hi, next));
        current[i] = next;

        return new FxRate(all[i], round6(next), System.currentTimeMillis());
    }

    private static double round6(double v) {
        return Math.round(v * 1_000_000.0) / 1_000_000.0;
    }
}
