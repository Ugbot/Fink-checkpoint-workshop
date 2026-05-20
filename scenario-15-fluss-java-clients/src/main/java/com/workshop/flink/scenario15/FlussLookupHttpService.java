package com.workshop.flink.scenario15;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.workshop.flink.common.fluss.FlussTables;
import org.apache.fluss.client.Connection;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.client.lookup.LookupResult;
import org.apache.fluss.client.lookup.Lookuper;
import org.apache.fluss.client.table.Table;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.row.GenericRow;
import org.apache.fluss.row.InternalRow;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * Scenario 15 — a microservice that exposes Fluss as a query API.
 *
 * <p>Two endpoints:
 * <pre>
 *   GET /accounts/{accountId}        → JSON of one row from account_profile
 *   GET /instruments/{isin}          → JSON of one row from instrument_master
 *   GET /health                      → "OK"
 * </pre>
 *
 * <p>This is the canonical "Fluss as the materialised state behind a
 * microservice" pattern — what people often try (and fail) to build on
 * top of Kafka compacted topics. With Fluss it's ~150 lines and uses
 * no external dependencies beyond the Fluss SDK + Jackson.
 *
 * <p>Run:
 * <pre>
 *   java -jar scenario-15-http-service-jar-with-dependencies.jar
 * </pre>
 *
 * <p>Test with:
 * <pre>
 *   curl http://localhost:18099/accounts/ACC-0042
 *   curl http://localhost:18099/instruments/ISIN00000042
 *   curl http://localhost:18099/health
 * </pre>
 *
 * <p>Env vars: {@code FLUSS_BOOTSTRAP}, {@code FLUSS_DATABASE}, {@code HTTP_PORT}.
 */
public class FlussLookupHttpService {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int DEFAULT_PORT = 18099;

    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", String.valueOf(DEFAULT_PORT)));
        String bootstrap = FlussTables.bootstrap();
        String database  = System.getenv().getOrDefault("FLUSS_DATABASE", FlussTables.DATABASE);

        Configuration conf = new Configuration();
        conf.setString("bootstrap.servers", bootstrap);
        Connection conn = ConnectionFactory.createConnection(conf);

        // Table + Lookuper are NOT thread-safe per the Fluss docs, so create
        // one per HTTP server. The HttpServer dispatches requests on a fixed
        // executor so each handler is single-threaded by the listener — but
        // for safety we synchronise on the lookuper in the handlers.
        Table accountTable    = conn.getTable(TablePath.of(database, FlussTables.TABLE_ACCOUNT_PROFILE));
        Table instrumentTable = conn.getTable(TablePath.of(database, FlussTables.TABLE_INSTRUMENT_MASTER));

        Lookuper accountLookup    = accountTable.newLookup().createLookuper();
        Lookuper instrumentLookup = instrumentTable.newLookup().createLookuper();

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));

        server.createContext("/accounts/", new AccountHandler(accountLookup));
        server.createContext("/instruments/", new InstrumentHandler(instrumentLookup));
        server.createContext("/health", exch -> respond(exch, 200, "OK\n", "text/plain"));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { server.stop(0); } catch (Throwable ignored) {}
            try { accountTable.close(); }    catch (Throwable ignored) {}
            try { instrumentTable.close(); } catch (Throwable ignored) {}
            try { conn.close(); }            catch (Throwable ignored) {}
        }));

        System.out.println("FlussLookupHttpService listening on port " + port
                + " (Fluss=" + bootstrap + ", db=" + database + ")");
        server.start();
    }

    // ── /accounts/{accountId} ─────────────────────────────────────────────

    static class AccountHandler implements HttpHandler {
        private final Lookuper lookuper;
        AccountHandler(Lookuper lookuper) { this.lookuper = lookuper; }
        @Override
        public void handle(HttpExchange exch) throws IOException {
            String path = exch.getRequestURI().getPath();   // /accounts/ACC-0042
            String key  = path.substring(path.lastIndexOf('/') + 1);
            if (key.isEmpty()) { respond(exch, 400, "missing accountId\n", "text/plain"); return; }
            try {
                LookupResult res;
                synchronized (lookuper) { res = lookuper.lookup(GenericRow.of(key)).get(); }
                List<InternalRow> rows = res.getRowList();
                if (rows == null || rows.isEmpty()) {
                    respond(exch, 404, "{\"error\":\"not_found\",\"accountId\":\"" + key + "\"}\n", "application/json");
                    return;
                }
                InternalRow row = rows.get(0);
                ObjectNode json = MAPPER.createObjectNode();
                json.put("account_id",      row.getString(0).toString());
                json.put("customer_id",     row.getString(1).toString());
                json.put("account_name",    row.getString(2).toString());
                json.put("tier",            row.getString(5).toString());
                json.put("region",          row.getString(6).toString());
                json.put("kyc_status",      row.getString(11).toString());
                json.put("aml_status",      row.getString(13).toString());
                json.put("risk_score",      row.getInt(14));
                json.put("daily_trade_limit",   row.getDouble(17));
                json.put("monthly_trade_limit", row.getDouble(18));
                respond(exch, 200, json.toString() + "\n", "application/json");
            } catch (Exception e) {
                respond(exch, 500, "{\"error\":\"" + e.getMessage() + "\"}\n", "application/json");
            }
        }
    }

    // ── /instruments/{isin} ───────────────────────────────────────────────

    static class InstrumentHandler implements HttpHandler {
        private final Lookuper lookuper;
        InstrumentHandler(Lookuper lookuper) { this.lookuper = lookuper; }
        @Override
        public void handle(HttpExchange exch) throws IOException {
            String path = exch.getRequestURI().getPath();
            String key  = path.substring(path.lastIndexOf('/') + 1);
            if (key.isEmpty()) { respond(exch, 400, "missing isin\n", "text/plain"); return; }
            try {
                LookupResult res;
                synchronized (lookuper) { res = lookuper.lookup(GenericRow.of(key)).get(); }
                List<InternalRow> rows = res.getRowList();
                if (rows == null || rows.isEmpty()) {
                    respond(exch, 404, "{\"error\":\"not_found\",\"isin\":\"" + key + "\"}\n", "application/json");
                    return;
                }
                InternalRow row = rows.get(0);
                ObjectNode json = MAPPER.createObjectNode();
                // Project a handful of columns — much faster than reading all 45.
                json.put("isin",         row.getString(0).toString());
                json.put("ticker",       row.getString(3).toString());
                json.put("exchange",     row.getString(4).toString());
                json.put("sector",       row.getString(7).toString());
                json.put("country",      row.getString(9).toString());
                json.put("currency",     row.getString(10).toString());
                json.put("last_close",   row.getDouble(17));
                json.put("beta_1y",      row.getDouble(23));
                json.put("market_cap_usd", row.getDouble(30));
                respond(exch, 200, json.toString() + "\n", "application/json");
            } catch (Exception e) {
                respond(exch, 500, "{\"error\":\"" + e.getMessage() + "\"}\n", "application/json");
            }
        }
    }

    private static void respond(HttpExchange exch, int code, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exch.getResponseHeaders().add("Content-Type", contentType);
        exch.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = exch.getResponseBody()) { os.write(bytes); }
    }
}
