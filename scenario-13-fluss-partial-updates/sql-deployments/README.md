# Deployable variants — Scenario 13

The SQL teaching files in [`../sql/`](../sql/) are written for the SQL Client.
Most of them are one-shot DDLs + small INSERT VALUES + verification SELECTs
— not a streaming workload by themselves.

The **deployable** form of scenario 13 is the Java DataStream job
`WideProfileUpdaterJob`, which reads a Kafka topic of mixed customer
events and partial-updates the Fluss `customer_360` table.

| Step | Why no deployable SQL |
|---|---|
| 01 — DDL inspection           | SHOW CREATE TABLE — interactive |
| 02 — KYC partial update       | INSERT VALUES — one-shot |
| 03 — AML + Marketing slices   | INSERT VALUES — one-shot |
| 04 — Aggregation merge engine | INSERT VALUES — one-shot |
| 05 — first_row + versioned    | INSERT VALUES — one-shot |
| —    WideProfileUpdaterJob     | ✓ shipped as a JAR (see ../README.md) |
