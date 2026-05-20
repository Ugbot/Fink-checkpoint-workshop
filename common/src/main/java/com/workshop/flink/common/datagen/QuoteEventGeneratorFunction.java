package com.workshop.flink.common.datagen;

import com.workshop.flink.common.model.QuoteEvent;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.concurrent.ThreadLocalRandom;

public class QuoteEventGeneratorFunction implements GeneratorFunction<Long, QuoteEvent> {

    private static final long serialVersionUID = 1L;

    /** Must match TradeEventGeneratorFunction.TICKERS so trade↔quote joins find matches. */
    private static final String[] TICKERS = {
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
        "JPM",  "BAC",  "GS",    "MS",   "WFC"
    };

    /** Mid prices roughly aligned with the trade generator's $10–$1000 range. */
    private static final double[] BASE_MID = {
        180.0, 410.0, 140.0, 175.0, 250.0,
        195.0,  35.0, 480.0,  95.0,  55.0
    };

    @Override
    public QuoteEvent map(Long index) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();
        int i = rng.nextInt(TICKERS.length);
        String ticker = TICKERS[i];

        // Mid = base ± 1.0%
        double mid    = BASE_MID[i] * (1.0 + rng.nextDouble(-0.01, 0.01));
        double spread = mid * 0.0005;             // 5 bps spread
        double bid    = round2(mid - spread / 2);
        double ask    = round2(mid + spread / 2);

        return QuoteEvent.of(ticker, bid, ask);
    }

    private static double round2(double v) {
        return Math.round(v * 100.0) / 100.0;
    }
}
