package com.workshop.flink.common.datagen;

import com.workshop.flink.common.model.AccountProfile;
import com.workshop.flink.common.model.CustomerAml;
import com.workshop.flink.common.model.CustomerCore;
import com.workshop.flink.common.model.CustomerMarketing;
import com.workshop.flink.common.model.InstrumentMaster;
import com.workshop.flink.common.model.TradeWide;
import org.apache.flink.connector.datagen.source.GeneratorFunction;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Self-contained {@link GeneratorFunction} implementations for the wide
 * financial domain. Each generator is deterministic only in its column set
 * — values are randomised every call so the resulting Fluss tables look
 * realistic across re-runs.
 *
 * <p>The generators don't need any shared state, so they're safe to use at
 * any parallelism. The {@code index} arg is used by some to pick stable IDs
 * (e.g. {@code ISIN-NNNNNN}) so successive seed runs hit overlapping primary
 * keys and the partial-update demos see merges, not pure inserts.
 */
public final class WideFinancialGenerators {

    private WideFinancialGenerators() {}

    private static final String[] EXCHANGES   = {"NASDAQ", "NYSE", "LSE", "TSE", "XETR", "ASX"};
    private static final String[] MICS        = {"XNAS", "XNYS", "XLON", "XTKS", "XETR", "XASX"};
    private static final String[] SEC_TYPES   = {"EQUITY", "BOND", "ETF", "OPTION"};
    private static final String[] SECTORS     = {"Technology", "Financials", "Health Care",
            "Consumer Discretionary", "Industrials", "Energy", "Utilities",
            "Materials", "Real Estate", "Communication Services"};
    private static final String[] SUB_SECTORS = {"Software", "Semiconductors", "Banks",
            "Insurance", "Biotech", "Pharma", "Autos", "Retail", "Aerospace"};
    private static final String[] COUNTRIES   = {"US", "GB", "DE", "JP", "AU", "FR", "CA"};
    private static final String[] CURRENCIES  = {"USD", "EUR", "GBP", "JPY", "AUD"};

    // ── Instrument master (PK = isin) ────────────────────────────────────────

    public static class InstrumentMasterGen implements GeneratorFunction<Long, InstrumentMaster> {
        private static final long serialVersionUID = 1L;
        private final int universeSize;
        public InstrumentMasterGen(int universeSize) { this.universeSize = universeSize; }
        @Override
        public InstrumentMaster map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            // Pick a stable ISIN from a bounded universe so re-runs overwrite the same rows.
            int i = (int) Math.floorMod(index, universeSize);
            InstrumentMaster m = new InstrumentMaster();
            m.setIsin(String.format("ISIN%08d", i));
            m.setCusip(String.format("CUSIP%06d", i));
            m.setSedol(String.format("SED%06d", i));
            m.setTicker("TKR" + i);
            int exIdx = rng.nextInt(EXCHANGES.length);
            m.setExchange(EXCHANGES[exIdx]);
            m.setMic(MICS[exIdx]);
            m.setSecurityType(SEC_TYPES[rng.nextInt(SEC_TYPES.length)]);
            m.setSector(SECTORS[rng.nextInt(SECTORS.length)]);
            m.setSubSector(SUB_SECTORS[rng.nextInt(SUB_SECTORS.length)]);
            m.setCountry(COUNTRIES[rng.nextInt(COUNTRIES.length)]);
            m.setCurrency(CURRENCIES[rng.nextInt(CURRENCIES.length)]);
            m.setLotSize(rng.nextInt(1, 100));
            m.setTickSize(0.01);
            m.setMinNotional(rng.nextDouble(100, 1000));
            m.setMaxOrderQty(rng.nextDouble(1e4, 1e6));
            m.setShortable(rng.nextBoolean());
            m.setMarginable(rng.nextBoolean());
            m.setLastClose(round2(rng.nextDouble(10, 1000)));
            m.setFiftyTwoWeekHigh(round2(m.getLastClose() * 1.3));
            m.setFiftyTwoWeekLow(round2(m.getLastClose() * 0.7));
            m.setPrevDayVolume(rng.nextDouble(1e4, 1e7));
            m.setAvgDailyVolume30d(m.getPrevDayVolume() * 0.95);
            m.setAvgDailyVolume90d(m.getPrevDayVolume() * 0.90);
            m.setBeta1y(round4(rng.nextDouble(0.5, 1.8)));
            m.setBeta3y(round4(rng.nextDouble(0.5, 1.8)));
            m.setBeta5y(round4(rng.nextDouble(0.5, 1.8)));
            m.setSectorBeta(round4(rng.nextDouble(0.7, 1.3)));
            m.setHedgeRatio(round4(rng.nextDouble(0.0, 1.0)));
            m.setVolatility30d(round4(rng.nextDouble(0.05, 0.6)));
            m.setValueAtRisk95(round4(rng.nextDouble(0.01, 0.10)));
            m.setMarketCapUsd(round2(rng.nextDouble(1e8, 1e12)));
            m.setFreeFloat(round4(rng.nextDouble(0.3, 1.0)));
            m.setDividendYield(round4(rng.nextDouble(0.0, 0.08)));
            m.setPriceEarnings(round2(rng.nextDouble(5, 60)));
            m.setPriceBook(round2(rng.nextDouble(0.5, 15)));
            m.setDebtToEquity(round4(rng.nextDouble(0.0, 3.0)));
            m.setEsgScore(round2(rng.nextDouble(10, 90)));
            m.setEnvironmentalScore(round2(rng.nextDouble(10, 90)));
            m.setSocialScore(round2(rng.nextDouble(10, 90)));
            m.setGovernanceScore(round2(rng.nextDouble(10, 90)));
            m.setListedAt(System.currentTimeMillis() - rng.nextLong(365L * 86_400_000L * 20));
            m.setUpdatedAt(System.currentTimeMillis());
            m.setIssuerName("Issuer-" + i);
            m.setIssuerLei(String.format("LEI%017d", i));
            m.setActiveFlag(true);
            m.setRegulatoryStatus("ACTIVE");
            m.setSourceSystem("instr-master-svc");
            return m;
        }
    }

    // ── Account profile (PK = accountId) ─────────────────────────────────────

    private static final String[] TIERS   = {"PLATINUM", "GOLD", "SILVER", "BRONZE"};
    private static final String[] REGIONS = {"NA", "EMEA", "APAC", "LATAM"};
    private static final String[] MIFID   = {"RETAIL", "PROFESSIONAL", "ELIGIBLE_COUNTERPARTY"};

    public static class AccountProfileGen implements GeneratorFunction<Long, AccountProfile> {
        private static final long serialVersionUID = 1L;
        private final int universeSize;
        public AccountProfileGen(int universeSize) { this.universeSize = universeSize; }
        @Override
        public AccountProfile map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int i = (int) Math.floorMod(index, universeSize);
            AccountProfile a = new AccountProfile();
            a.setAccountId(String.format("ACC-%04d", i + 1));
            a.setCustomerId(String.format("CUST-%06d", i + 1));
            a.setAccountName("Account-" + i);
            a.setAccountType(rng.nextBoolean() ? "INDIVIDUAL" : "CORPORATE");
            a.setCurrency(CURRENCIES[rng.nextInt(CURRENCIES.length)]);
            a.setTier(TIERS[rng.nextInt(TIERS.length)]);
            a.setRegion(REGIONS[rng.nextInt(REGIONS.length)]);
            a.setCountry(COUNTRIES[rng.nextInt(COUNTRIES.length)]);
            a.setMifidClassification(MIFID[rng.nextInt(MIFID.length)]);
            a.setFatcaStatus(rng.nextBoolean() ? "COMPLIANT" : "NOT_APPLICABLE");
            a.setTaxResidency(a.getCountry());
            a.setKycStatus("VERIFIED");
            a.setKycVerifiedAt(System.currentTimeMillis() - rng.nextLong(365L * 86_400_000L));
            a.setAmlStatus("CLEAR");
            a.setRiskScore(rng.nextInt(0, 100));
            a.setPepFlag(false);
            a.setSanctionsFlag(false);
            a.setDailyTradeLimit(rng.nextDouble(1e4, 1e7));
            a.setMonthlyTradeLimit(a.getDailyTradeLimit() * 20);
            a.setCreditLine(rng.nextDouble(1e3, 1e6));
            a.setAvailableBalance(rng.nextDouble(100, 1e6));
            a.setMarginUsed(rng.nextDouble(0, a.getCreditLine()));
            a.setIban(String.format("GB%02d-WSHP-%010d", rng.nextInt(10, 99), i));
            a.setSwiftBic("WSHPGB22");
            a.setCustodianBank("Workshop Custody Ltd");
            a.setOpenedAt(System.currentTimeMillis() - rng.nextLong(365L * 86_400_000L * 5));
            a.setLastLoginAt(System.currentTimeMillis() - rng.nextLong(86_400_000L * 30));
            a.setUpdatedAt(System.currentTimeMillis());
            a.setActiveFlag(true);
            a.setSourceSystem("account-svc");
            return a;
        }
    }

    // ── Partial-update sources for customer_360 ──────────────────────────────

    public static class CustomerCoreGen implements GeneratorFunction<Long, CustomerCore> {
        private static final long serialVersionUID = 1L;
        private final int universeSize;
        public CustomerCoreGen(int universeSize) { this.universeSize = universeSize; }
        @Override
        public CustomerCore map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int i = (int) Math.floorMod(index, universeSize);
            CustomerCore c = new CustomerCore();
            c.setCustomerId(String.format("CUST-%06d", i + 1));
            c.setLegalName("Customer " + i);
            c.setDateOfBirth(String.format("19%02d-%02d-%02d",
                    rng.nextInt(50, 99), rng.nextInt(1, 13), rng.nextInt(1, 29)));
            c.setNationality(COUNTRIES[rng.nextInt(COUNTRIES.length)]);
            c.setTaxResidency(c.getNationality());
            c.setKycStatus(rng.nextDouble() < 0.95 ? "VERIFIED" : "PENDING");
            c.setKycVerifiedAt(System.currentTimeMillis() - rng.nextLong(365L * 86_400_000L));
            c.setIdDocumentType(rng.nextBoolean() ? "PASSPORT" : "NATIONAL_ID");
            c.setIdDocumentNumber(UUID.randomUUID().toString().substring(0, 12));
            c.setAddressLine(rng.nextInt(1, 999) + " High Street");
            c.setAddressCountry(c.getNationality());
            c.setUpdatedAt(System.currentTimeMillis());
            return c;
        }
    }

    public static class CustomerAmlGen implements GeneratorFunction<Long, CustomerAml> {
        private static final long serialVersionUID = 1L;
        private final int universeSize;
        public CustomerAmlGen(int universeSize) { this.universeSize = universeSize; }
        @Override
        public CustomerAml map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int i = (int) Math.floorMod(index, universeSize);
            CustomerAml a = new CustomerAml();
            a.setCustomerId(String.format("CUST-%06d", i + 1));
            // 95% CLEAR, 3% UNDER_REVIEW, 2% FLAGGED — realistic AML mix
            double d = rng.nextDouble();
            a.setAmlStatus(d < 0.95 ? "CLEAR" : (d < 0.98 ? "UNDER_REVIEW" : "FLAGGED"));
            a.setRiskScore(rng.nextInt(0, 100));
            a.setPepFlag(rng.nextDouble() < 0.005);
            a.setSanctionsFlag(rng.nextDouble() < 0.001);
            a.setAmlReviewedBy("aml-bot-" + rng.nextInt(1, 5));
            a.setAmlReviewedAt(System.currentTimeMillis());
            a.setUpdatedAt(System.currentTimeMillis());
            return a;
        }
    }

    private static final String[] CHANNELS = {"EMAIL", "SMS", "PUSH", "NONE"};
    private static final String[] SEGMENTS = {"GROWTH", "RETAIN", "DORMANT", "VIP"};
    private static final String[] ACQ_SRC  = {"ORGANIC", "PAID_SOCIAL", "REFERRAL", "PARTNER"};

    public static class CustomerMarketingGen implements GeneratorFunction<Long, CustomerMarketing> {
        private static final long serialVersionUID = 1L;
        private final int universeSize;
        public CustomerMarketingGen(int universeSize) { this.universeSize = universeSize; }
        @Override
        public CustomerMarketing map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int i = (int) Math.floorMod(index, universeSize);
            CustomerMarketing m = new CustomerMarketing();
            m.setCustomerId(String.format("CUST-%06d", i + 1));
            m.setMarketingOptIn(rng.nextBoolean());
            m.setEmailOptIn(m.isMarketingOptIn() && rng.nextBoolean());
            m.setSmsOptIn(m.isMarketingOptIn() && rng.nextDouble() < 0.4);
            m.setPreferredChannel(CHANNELS[rng.nextInt(CHANNELS.length)]);
            m.setSegment(SEGMENTS[rng.nextInt(SEGMENTS.length)]);
            m.setAcquisitionSource(ACQ_SRC[rng.nextInt(ACQ_SRC.length)]);
            m.setLastEngagementAt(System.currentTimeMillis() - rng.nextLong(86_400_000L * 90));
            m.setUpdatedAt(System.currentTimeMillis());
            return m;
        }
    }

    // ── Wide trade log (append-only) ─────────────────────────────────────────

    private static final String[] ORDER_TYPES = {"MARKET", "LIMIT", "STOP", "STOP_LIMIT"};
    private static final String[] DESKS       = {"EQ-CASH", "EQ-DERIV", "FX", "RATES"};

    public static class TradeWideGen implements GeneratorFunction<Long, TradeWide> {
        private static final long serialVersionUID = 1L;
        private final int instrumentUniverse;
        private final int accountUniverse;
        public TradeWideGen(int instrumentUniverse, int accountUniverse) {
            this.instrumentUniverse = instrumentUniverse;
            this.accountUniverse    = accountUniverse;
        }
        @Override
        public TradeWide map(Long index) {
            ThreadLocalRandom rng = ThreadLocalRandom.current();
            int instr = rng.nextInt(0, instrumentUniverse);
            TradeWide t = new TradeWide();
            t.setEventId(UUID.randomUUID().toString());
            t.setAccountId(String.format("ACC-%04d", rng.nextInt(1, accountUniverse + 1)));
            t.setCustomerId(String.format("CUST-%06d", rng.nextInt(1, accountUniverse + 1)));
            t.setTicker("TKR" + instr);
            t.setIsin(String.format("ISIN%08d", instr));
            t.setSide(rng.nextBoolean() ? "BUY" : "SELL");
            t.setOrderType(ORDER_TYPES[rng.nextInt(ORDER_TYPES.length)]);
            t.setQuantity(rng.nextInt(1, 5000));
            t.setPrice(round2(rng.nextDouble(10, 1000)));
            t.setNotional(round2(t.getQuantity() * t.getPrice()));
            t.setCurrency(CURRENCIES[rng.nextInt(CURRENCIES.length)]);
            double fx;
            switch (t.getCurrency()) {
                case "EUR": fx = 1.09;   break;
                case "GBP": fx = 1.27;   break;
                case "JPY": fx = 0.0063; break;
                case "AUD": fx = 0.66;   break;
                default:    fx = 1.0;
            }
            t.setFxRateToUsd(round4(fx));
            t.setNotionalUsd(round2(t.getNotional() * t.getFxRateToUsd()));
            t.setExchange(EXCHANGES[rng.nextInt(EXCHANGES.length)]);
            t.setVenue(t.getExchange() + "-MAIN");
            t.setSector(SECTORS[rng.nextInt(SECTORS.length)]);
            t.setCountry(COUNTRIES[rng.nextInt(COUNTRIES.length)]);
            t.setDesk(DESKS[rng.nextInt(DESKS.length)]);
            t.setTrader("trader-" + rng.nextInt(1, 25));
            t.setTradeTime(System.currentTimeMillis());
            t.setSettledAt(t.getTradeTime() + 2L * 86_400_000L);   // T+2
            t.setSettlementStatus("PENDING");
            t.setCommission(round2(t.getNotional() * 0.0005));
            t.setTax(round2(t.getNotional() * 0.0001));
            t.setSourceApp("trade-router");
            return t;
        }
    }

    private static double round2(double v) { return Math.round(v * 100.0) / 100.0; }
    private static double round4(double v) { return Math.round(v * 10_000.0) / 10_000.0; }
}
