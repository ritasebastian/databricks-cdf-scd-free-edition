# Databricks Delta CDF → Structured Streaming → SCD Type 1 & 2 (Free Edition)

Notebook-first **hands-on** project modeled after the same repo layout and learning flow as the Databricks Auto Loader Free Edition demonstration: a full `.ipynb`, a Databricks source `.py`, input fixtures, detailed walkthrough and concepts.

## What you'll prove

1. Insert four Bronze customers with Change Data Feed enabled.
2. Stream CDF to two Delta Silver tables via `foreachBatch` + Delta `MERGE`: SCD1 and SCD2.
3. Perform two updates and one delete in three separate Delta commits.
4. Restart with the **same checkpoint**: automatically update Silver.
5. Restart again with no new changes: **no duplicate SCD2 history**.
6. Validate SCD1=**4** current state rows (including a soft-delete), SCD2=**7** history rows.

## Project layout

```text
databricks-cdf-scd-free-edition/
├── README.md
├── .gitignore
├── data/
│   ├── initial_customers.csv
│   └── change_events.csv
├── notebooks/
│   ├── cdf_scd_complete_demo.ipynb   # preferred Databricks import
│   └── cdf_scd_complete_demo.py      # Databricks source notebook
└── docs/
    ├── WALKTHROUGH.md
    └── concepts.md
```

## Prerequisites

- Databricks workspace with Unity Catalog, Delta Lake CDF, Structured Streaming `foreachBatch` and SQL `MERGE`.
- Free Edition with serverless is intended; quota and access limits may apply. No cloud credentials or pip installs needed.
- Permission to create a schema, Volume and managed tables in a writable catalog.

## Fastest run (one notebook)

1. Download ZIP, extract locally.
2. Databricks **Workspace → Import** `notebooks/cdf_scd_complete_demo.ipynb`.
3. Attach **Serverless** compute. In cell 01 change `catalog_name = "workspace"` if your catalog is different (`SHOW CATALOGS`).
4. Run cells **in order**, observing outputs after every commit and stream run. Start with an unused demo schema.
5. At the end, save screenshots after assertions pass.

**Important:** Run the lab from a fresh demo state. Restarting the notebook from cell 01 after the full lab without reset is intentionally rejected because the seed data would be replayed. Do not reset the checkpoint while retaining Silver data.

## Expected results

| Customer | Bronze final | Silver SCD1 final | SCD2 versions |
| --- | --- | --- | ---: |
| 101 Alice | Portland | Portland; not deleted | 2 |
| 102 Bob | Austin | Austin; not deleted | 2 |
| 103 Cara | deleted from Bronze | Miami; `is_deleted=true` | 2 |
| 104 Dan | Boston | Boston; not deleted | 1 |
| **Total** | **3 live Bronze rows** | **4 rows** | **7 rows** |

The CDF update preimages are *inspected but not merged*; only inserts, update postimages, and deletes represent new business state.

## Publishing to GitHub

### Easiest: website

1. Create a new empty repository `databricks-cdf-scd-demo` (do not initialize with README).
2. Unzip the project and upload the **contents** of its root directory to GitHub (keep `data/`, `docs/`, `notebooks/` folders).
3. Commit `Add Databricks CDF Structured Streaming SCD1 SCD2 demo`.
4. Add your own proof screenshots only after executing the notebook successfully.

### Git CLI

```bash
cd databricks-cdf-scd-free-edition
git init
git add .
git commit -m "Add Databricks CDF streaming SCD1 SCD2 demo"
git branch -M main
git remote add origin https://github.com/ritasebastian/databricks-cdf-scd-demo.git
git push -u origin main
```

Create the empty GitHub remote first. The archive includes no real customer data or credentials.

## How it works

```text
Bronze Delta (CDF enabled)
  ├─ INSERT snapshot commit
  ├─ UPDATE commits → preimage + postimage
  └─ DELETE commit
      ↓ spark.readStream(readChangeFeed=true)
  foreachBatch (group by _commit_version, ascending)
      ├─ SCD1 MERGE: current values + deleted marker
      └─ SCD2 MERGE: expire prior current version + insert history/tombstone
             ↓
       checkpoint in UC Volume
```

## Troubleshooting

- **Catalog not found:** run `SHOW CATALOGS`; set `catalog_name` in first cell.
- **No permission for Volume:** use catalog/schema where you have `CREATE VOLUME`; ask admin if needed.
- **Tables already populated:** use a new schema name or carefully execute the commented reset in cell 12.
- **Checkpoint errors:** don't delete and re-create checkpoints casually. For a full reset, delete the three demo tables and the checkpoint.
- **Already ran initial inserts:** skip cells 01–03 and continue from the next unexecuted cell; avoid duplicate seed rows.
- **Merges fail:** check privileges, your runtime, and source uniqueness. Source events are applied one Delta commit at a time.

## Scope and validation

Notebook code and JSON syntax are statically checked during packaging, but **no live Databricks run is claimed**. Test it in your workspace before representing the demo as fully verified. It is an educational, single-writer example, **not** a fault-atomic, production-grade multi-table transaction. Delta CDF has retention limits.

## Official references

- [Databricks CDF documentation](https://docs.databricks.com/aws/en/tables/features/change-data-feed)
- [Databricks Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Databricks serverless limitations](https://docs.databricks.com/aws/en/compute/serverless/limitations)
