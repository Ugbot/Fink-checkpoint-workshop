# Deployable variants — Scenario 16

Most of scenario 16 is non-deployable in the SQL-deployment sense:
- Step 01 is DDL.
- Steps 02–04 are inspection/query SQL (interactive).
- Steps 05–06 are the **Spark Java apps**, submitted via `spark-submit`
  inside `workshop-spark`, not through the Flink/VVC deployment harness.
- Step 07 is a Markdown discussion.

The Java apps live in [`../src/main/java/com/workshop/flink/scenario16/`](../src/main/java/com/workshop/flink/scenario16/).
Use [`../../scripts/spark-submit-scenario-16.sh`](../../scripts/spark-submit-scenario-16.sh)
to run them.
