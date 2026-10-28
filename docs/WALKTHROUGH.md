# Step-by-step walkthrough

1. Choose `workspace` or another writable catalog in notebook cell 01.
2. Create fresh Bronze/Silver managed Delta tables in cell 02 (CDF is enabled).
3. First SQL INSERT: Bronze contains four customers.
4. Define CDF Spark Structured Streaming `foreachBatch` MERGE in cell 04.
5. `availableNow=True` processes all available records, writes 4 SCD1 and 4 SCD2 rows.
6. Execute two UPDATE commits and a DELETE commit.
7. Inspect CDF rows (preimage, postimage, delete).
8. Re-run stream using the same checkpoint: Silver SCD1=4, history SCD2=7.
9. Re-run with no Bronze changes: counts stay 4 and 7.
10. Run assertions, inspect timeline and test DELETE tombstone.

## Suggested screenshots for GitHub

- CDF event table including `_change_type` and `_commit_version`
- SCD1 after mutations (Portland, Austin, deleted Cara)
- SCD2 history including old/new versions, `is_current`, timestamps
- Final `ALL TESTS PASSED` output **only after** your own Databricks execution

## Why the sample CSVs are included

They document the input data, analogous to the uploaded Auto Loader demo's `data/` folder. Here the notebook generates SQL INSERT/UPDATE/DELETE commits directly, rather than ingesting the CSVs: actual Delta CDF is produced by those table commits.
