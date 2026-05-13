package com.workshop.flink.scenario06;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Scenario 06 — Paimon catalog/table bootstrap.
 *
 * Runs only the DDL (CREATE CATALOG / DATABASE / TABLE) so that the SQL Gateway
 * sees a non-empty schema even before any ingest job has run. Idempotent.
 *
 * Submit with: flink run scenario-06-bootstrap-jar-with-dependencies.jar
 */
public class PaimonCatalogBootstrap {

    private static final Logger LOG = LogManager.getLogger(PaimonCatalogBootstrap.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Bootstrapping Paimon catalog at {}", PaimonConfig.warehouse());

        TableEnvironment tEnv = TableEnvironment.create(
            EnvironmentSettings.inBatchMode());

        tEnv.executeSql(PaimonConfig.createCatalogSql());
        tEnv.executeSql(PaimonConfig.useCatalogSql());
        tEnv.executeSql(PaimonConfig.createDatabaseSql());
        tEnv.executeSql("USE " + PaimonConfig.DATABASE);
        tEnv.executeSql(PaimonConfig.createTableSql());

        LOG.info("Paimon catalog bootstrap complete: catalog={}, db={}, table={}",
            PaimonConfig.CATALOG, PaimonConfig.DATABASE, PaimonConfig.TABLE);

        tEnv.executeSql("SHOW TABLES").print();
    }
}
