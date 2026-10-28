# Key concepts

**CDF:** Delta emits `insert`, `update_preimage`, `update_postimage`, and `delete` operations with commit metadata. CDF must be enabled before changes to capture them. A new streaming read without `startingVersion` returns the latest table snapshot as inserts, followed by later changes; this notebook starts from an empty Bronze table and creates changes after the tables exist.

**Structured Streaming:** `spark.readStream.option("readChangeFeed", "true").table(bronze)` with `.writeStream.foreachBatch(...)` and durable `checkpointLocation`. `availableNow=True` drains currently available changes and exits; repeated runs use the checkpoint.

**SCD1:** One row/customer; updates replace attributes, deletes preserve a soft-delete marker to avoid resurrection on replay.

**SCD2:** Each processed change gets a row with `[valid_from, valid_to)` and `is_current`. A DELETE is a tombstone row, so only `is_deleted = false` represents an active business customer.

**Ordering:** A microbatch can contain multiple commits; process the commit versions in increasing order. The demo expects at most one final event/customer/commit. SCD2 uses two merges: expire and insert. The event key `(customer_id, source_version)` prevents duplicate history after a retry.

**Limits:** A crash between the two SCD2 MERGEs can temporarily leave a missing current row; retry heals it. These are not a cross-table atomic transaction. This educational lab assumes a single writer, schema stability, unique business keys, and retained CDF history. For production, consider Lakeflow declarative pipelines, CDC sequencing, change event archival, backfills, monitoring, and CDF retention strategy.
