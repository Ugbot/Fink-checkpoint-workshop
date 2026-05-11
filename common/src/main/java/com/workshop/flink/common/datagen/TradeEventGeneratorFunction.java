package com.workshop.flink.common.datagen;

import com.workshop.flink.common.model.TradeEvent;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.concurrent.ThreadLocalRandom;

public class TradeEventGeneratorFunction implements GeneratorFunction<Long, TradeEvent> {

    private static final long serialVersionUID = 1L;

    private static final String[] TICKERS = {
        "AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
        "JPM",  "BAC",  "GS",    "MS",   "WFC"
    };

    private static final String[] SIDES = {"BUY", "SELL"};

    @Override
    public TradeEvent map(Long index) {
        ThreadLocalRandom rng = ThreadLocalRandom.current();

        String ticker   = TICKERS[rng.nextInt(TICKERS.length)];
        String side     = SIDES[rng.nextInt(2)];
        int    quantity = rng.nextInt(1, 1001);
        double rawPrice = 10.0 + rng.nextDouble(990.0);
        double price    = Math.round(rawPrice * 100.0) / 100.0;
        String acct     = String.format("ACC-%04d", rng.nextInt(1, 51));

        return TradeEvent.of(acct, ticker, side, quantity, price);
    }
}
