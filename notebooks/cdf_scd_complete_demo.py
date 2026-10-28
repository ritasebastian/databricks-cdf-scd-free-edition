# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC **Free Edition / Serverless-friendly walkthrough.** Execute each cell in order. This notebook generates sample changes using Delta SQL, reads Bronze Change Data Feed with Spark Structured Streaming, and performs two automated Delta MERGEs for each commit. No cloud keys or external data sources.


# COMMAND ----------

# MAGIC %md
# MAGIC Set the catalog to one you can write to (usually `workspace` on Free Edition). This creates a **separate demo schema**; it will not touch your previous projects. Only use the optional reset section when intentionally restarting.


# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

catalog_name = "workspace"  # change after SHOW CATALOGS if necessary
schema_name = "cdf_scd_free_demo"
volume_name = "cdf_demo_runtime"

# Names here are fixed identifiers; use simple catalog/schema names, not arbitrary SQL.
assert catalog_name.replace("_", "").isalnum()
assert schema_name.replace("_", "").isalnum()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog_name}`.`{schema_name}`.`{volume_name}`")
namespace = f"`{catalog_name}`.`{schema_name}`"
bronze = f"{namespace}.bronze_customers"
scd1 = f"{namespace}.silver_customers_scd1"
scd2 = f"{namespace}.silver_customers_scd2"
checkpoint = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/checkpoints/cdf_to_silver_v1"
print("Bronze:", bronze, " | SCD1:", scd1, " | SCD2:", scd2, " | Checkpoint:", checkpoint)


# COMMAND ----------

# MAGIC %md
# MAGIC CDF must be enabled **before** the demo writes. SCD1 stores latest customer state, including a deleted flag. SCD2 stores historical versions; `valid_to` is exclusive and a deleted row is kept as an explicit tombstone.


# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS {bronze} (
 customer_id BIGINT, customer_name STRING, email STRING, city STRING
) USING DELTA TBLPROPERTIES (delta.enableChangeDataFeed = true)""")
spark.sql(f"ALTER TABLE {bronze} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {scd1} (
 customer_id BIGINT, customer_name STRING, email STRING, city STRING,
 is_deleted BOOLEAN, source_version BIGINT, processed_at TIMESTAMP
) USING DELTA""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {scd2} (
 customer_id BIGINT, customer_name STRING, email STRING, city STRING,
 is_deleted BOOLEAN, valid_from TIMESTAMP, valid_to TIMESTAMP,
 is_current BOOLEAN, source_version BIGINT, processed_at TIMESTAMP
) USING DELTA""")
assert spark.sql(f"SELECT count(*) AS n FROM {bronze}").first().n == 0, "Bronze not empty. Use a fresh schema or follow reset instructions."
assert spark.sql(f"SELECT count(*) AS n FROM {scd1}").first().n == 0
assert spark.sql(f"SELECT count(*) AS n FROM {scd2}").first().n == 0
print("Created three empty Delta tables. Bronze CDF is enabled.")


# COMMAND ----------

# MAGIC %md
# MAGIC Keep the commits separate. The CDF consumer groups rows by Delta commit version, not merely microbatch ID.


# COMMAND ----------

spark.sql(f"""INSERT INTO {bronze} VALUES
 (101,'Alice','alice@example.test','Seattle'),
 (102,'Bob','bob@example.test','Dallas'),
 (103,'Cara','cara@example.test','Miami'),
 (104,'Dan','dan@example.test','Boston')""")
display(spark.sql(f"SELECT * FROM {bronze} ORDER BY customer_id"))


# COMMAND ----------

# MAGIC %md
# MAGIC A streaming microbatch may contain multiple commits. We process them in increasing `_commit_version` order and ignore `update_preimage`. For demonstration simplicity, each commit has at most one final event per customer. Each Silver merge has replay protection using `source_version`. If a retry occurs after expiring an SCD2 row but before inserting its successor, the insert merge completes the operation.


# COMMAND ----------

BUSINESS_COLUMNS = ["customer_id", "customer_name", "email", "city"]

def apply_one_commit(commit_df):
    changes = (commit_df
        .filter(F.col("_change_type").isin("insert", "update_postimage", "delete"))
        .select(*BUSINESS_COLUMNS, "_change_type", "_commit_version", "_commit_timestamp")
        .withColumn("is_deleted", F.col("_change_type") == "delete")
        .withColumnRenamed("_commit_version", "source_version")
        .withColumnRenamed("_commit_timestamp", "valid_from")
        .drop("_change_type")
        .cache())
    try:
        if not changes.take(1):
            return
        assert not changes.groupBy("customer_id").count().filter("count > 1").take(1), (
            "Multiple final changes per customer in one commit: not supported in this lab")
        # SCD Type 1: latest values + soft-delete tombstones; never regress on a retry.
        (DeltaTable.forName(spark, scd1).alias("t")
         .merge(changes.alias("s"), "t.customer_id = s.customer_id")
         .whenMatchedUpdate(condition="s.source_version > t.source_version", set={
             "customer_name": "s.customer_name", "email": "s.email", "city": "s.city",
             "is_deleted": "s.is_deleted", "source_version": "s.source_version",
             "processed_at": "current_timestamp()"})
         .whenNotMatchedInsert(values={
             "customer_id": "s.customer_id", "customer_name": "s.customer_name",
             "email": "s.email", "city": "s.city", "is_deleted": "s.is_deleted",
             "source_version": "s.source_version", "processed_at": "current_timestamp()"})
         .execute())
        # SCD Type 2 / phase A: expire only an older current row.
        (DeltaTable.forName(spark, scd2).alias("t")
         .merge(changes.alias("s"), "t.customer_id = s.customer_id AND t.is_current = true")
         .whenMatchedUpdate(condition="s.source_version > t.source_version", set={
             "valid_to": "s.valid_from", "is_current": "false",
             "processed_at": "current_timestamp()"})
         .execute())
        # SCD Type 2 / phase B: insert a version only if it does NOT already exist.
        # This uniquely identifies an event by (customer_id, source_version).
        (DeltaTable.forName(spark, scd2).alias("t")
         .merge(changes.alias("s"),
                "t.customer_id = s.customer_id AND t.source_version = s.source_version")
         .whenNotMatchedInsert(values={
             "customer_id": "s.customer_id", "customer_name": "s.customer_name",
             "email": "s.email", "city": "s.city", "is_deleted": "s.is_deleted",
             "valid_from": "s.valid_from", "valid_to": "CAST(NULL AS TIMESTAMP)",
             "is_current": "true", "source_version": "s.source_version",
             "processed_at": "current_timestamp()"})
         .execute())
    finally:
        changes.unpersist()

def merge_microbatch(batch_df, batch_id):
    versions = [r[0] for r in (batch_df.select("_commit_version")
                 .distinct().orderBy("_commit_version").collect())]
    for version in versions:
        apply_one_commit(batch_df.filter(F.col("_commit_version") == version))

def run_cdf_stream():
    changes = (spark.readStream.option("readChangeFeed", "true").table(bronze))
    query = (changes.writeStream.foreachBatch(merge_microbatch)
             .option("checkpointLocation", checkpoint)
             .trigger(availableNow=True).start())
    query.awaitTermination()
    if query.exception() is not None:
        raise RuntimeError(str(query.exception()))
    print("CDF stream completed. Last progress:", query.lastProgress)
    return query

print("Reusable Structured Streaming merge pipeline defined.")


# COMMAND ----------

# MAGIC %md


# COMMAND ----------

run_cdf_stream()
display(spark.sql(f"SELECT * FROM {scd1} ORDER BY customer_id"))
display(spark.sql(f"SELECT customer_id, city, is_deleted, valid_from, valid_to, is_current, source_version FROM {scd2} ORDER BY customer_id, source_version"))
assert spark.table(scd1).count() == 4
assert spark.table(scd2).count() == 4
print("PASS — initial snapshot: 4 SCD1 rows / 4 SCD2 history rows")


# COMMAND ----------

# MAGIC %md
# MAGIC Expected CDF: update preimage + postimage for each updated customer, and a delete event for Cara. We only apply postimages and deletes.


# COMMAND ----------

spark.sql(f"UPDATE {bronze} SET city = 'Portland' WHERE customer_id = 101")
spark.sql(f"UPDATE {bronze} SET city = 'Austin' WHERE customer_id = 102")
spark.sql(f"DELETE FROM {bronze} WHERE customer_id = 103")
print("Bronze after UPDATE / DELETE:")
display(spark.sql(f"SELECT * FROM {bronze} ORDER BY customer_id"))


# COMMAND ----------

# MAGIC %md
# MAGIC `table_changes` includes metadata columns. The `update_preimage` records are informational and are **not** applied as the new Silver state.


# COMMAND ----------

display(spark.sql(f"""SELECT customer_id, city, _change_type, _commit_version, _commit_timestamp
FROM table_changes('{catalog_name}.{schema_name}.bronze_customers', 0)
ORDER BY _commit_version, customer_id, _change_type"""))


# COMMAND ----------

# MAGIC %md
# MAGIC Only the new commits should be processed. Current and historical Silver tables update automatically.


# COMMAND ----------

run_cdf_stream()
display(spark.sql(f"SELECT customer_id, customer_name, city, is_deleted, source_version FROM {scd1} ORDER BY customer_id"))
display(spark.sql(f"SELECT customer_id, city, is_deleted, valid_from, valid_to, is_current, source_version FROM {scd2} ORDER BY customer_id, source_version"))
assert spark.table(scd1).count() == 4
assert spark.table(scd2).count() == 7
print("PASS — SCD1: 4 rows; SCD2: 7 history rows including one delete tombstone")


# COMMAND ----------

# MAGIC %md


# COMMAND ----------

before = (spark.table(scd1).count(), spark.table(scd2).count())
run_cdf_stream()
after = (spark.table(scd1).count(), spark.table(scd2).count())
assert before == after == (4, 7), (before, after)
assert not spark.table(scd2).groupBy('customer_id', 'source_version').count().filter('count > 1').take(1)
assert not spark.table(scd2).filter('is_current').groupBy('customer_id').count().filter('count > 1').take(1)
print('PASS — zero duplicates on checkpoint restart:', before, after)


# COMMAND ----------

# MAGIC %md


# COMMAND ----------

current = {r.customer_id: r for r in spark.table(scd1).collect()}
assert current[101].city == 'Portland' and not current[101].is_deleted
assert current[102].city == 'Austin' and not current[102].is_deleted
assert current[103].is_deleted and current[103].city == 'Miami'
assert current[104].city == 'Boston' and not current[104].is_deleted
h = spark.table(scd2)
assert {r.customer_id: r.n for r in h.groupBy('customer_id').count().selectExpr('customer_id','count as n').collect()} == {101:2,102:2,103:2,104:1}
assert h.filter('is_current').count() == 4
assert h.filter('is_current AND is_deleted').count() == 1
assert h.filter('NOT is_current AND valid_to IS NULL').count() == 0
assert h.filter('is_current AND valid_to IS NOT NULL').count() == 0
print('ALL TESTS PASSED — Bronze CDF, SCD1, SCD2, DELETE, restart / replay protection')


# COMMAND ----------

# MAGIC %md
# MAGIC Add a new customer as a separate commit, run `run_cdf_stream()` again, and verify both Silver tables each add one row. The existing checkpoint will resume incrementally.


# COMMAND ----------

# OPTIONAL challenge — uncomment only once:
# spark.sql(f"INSERT INTO {bronze} VALUES (105, 'Eva', 'eva@example.test', 'Denver')")
# run_cdf_stream()
# display(spark.table(scd1))


# COMMAND ----------

# MAGIC %md
# MAGIC Only if you want to repeat the full lab from scratch: delete the *demo* checkpoint and drop the three demo tables. Do not delete the schema if you use it for anything else. **Uncomment in the order shown.**


# COMMAND ----------

# DANGER: destructive reset of this demo only. Never run while a query is active.
# dbutils.fs.rm(checkpoint, recurse=True)
# spark.sql(f"DROP TABLE IF EXISTS {scd2}")
# spark.sql(f"DROP TABLE IF EXISTS {scd1}")
# spark.sql(f"DROP TABLE IF EXISTS {bronze}")
# print("Reset complete. Run from cell 01 again.")
