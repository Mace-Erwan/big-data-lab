# Lab 6: Spark DataFrames - Answers

## SparkSession with S3 access

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**

The job stops and is lost. Spark runs inside the Onyxia service in `local[*]` mode, so if the service restarts,
Spark restarts too and everything in memory is gone. I would have to create the session again with the new
credentials and rerun my cells.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**

Because it only sends the access key and the secret key. The Onyxia credentials are temporary and also need the
session token, so S3 refuses the requests without it. `TemporaryAWSCredentialsProvider` sends the token too.

Note: the `pyspark` shell already starts a session, so I added `spark.stop()` before creating mine. Otherwise Spark
would have reused the existing session and ignored my S3 settings.

## Silver layer

**Explain the output of the code that was just run.**

It shows the number of users per state and how many have no city. Only `AE` (3 out of 3) and `AP` (2 out of 2) have
missing cities. These are military addresses, like `DPO AP 09617`, which have no city, so the parsing works as
expected. The `assert` lines printed nothing, so the DataFrame and SQL versions give the same result.

**Which one would you choose for a one-off exploration, and which for a model reused by several colleagues? Why?**

SQL for a quick exploration, because it's quick to write and easy to read. The DataFrame API for a shared model,
because it's Python code that I can organise into functions and reuse. Both give the same result in the end.

**Where else in this notebook could that same behavior silently produce a wrong result if left unhandled?**

In `state` and `zip_code`, which also use `regexp_extract()`. If an address didn't match, they would contain an empty
string instead of NULL, and a check for missing values would not see it. It didn't happen with my data, but nothing
would have warned me.

## Write the silver layer as Parquet

**How many part-files are there, and why?**

One part-file in each folder, plus a `_SUCCESS` file. Spark writes one file per partition, and my data was so small
that it fit in a single partition.

**`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?**

The parallelism. Only one task would write all the data, so it would be much slower, and it could run out of memory.

## Write the silver layer as Iceberg tables

**Which files hold the data, and which ones describe the table?**

In `silver/spark/orders.parquet/`, there are only data files. In `warehouse/silver/orders/`, the data is in `data/`
and the `metadata/` folder describes the table: `v1.metadata.json` for the schema and the versions, the Avro files
that list the data files, and `version-hint.text` that points to the current version.

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?**

With Parquet, the reader can see a mix of old and new files, or get an error. With Iceberg, the reader keeps reading
the version it started with, and the new version only appears once it is fully written.

**What can go wrong if two jobs commit at the same time, and what would a catalog service add?**

Both jobs could update the pointer at the same time, and one of the two changes could be lost. A catalog service
makes sure only one commit succeeds at a time, so the other one has to try again.

## Compare with the dbt lab

**What differs between the two schemas? What did you do about the UUID type, and what is the consequence?**

With dbt, `order_id` and `user_id` are `uuid` and `ordered_at` has a time zone. With Spark, the IDs are `varchar` and
`ordered_at` is a plain `timestamp`. I kept the IDs as text. The file is bigger (140 KB against 72 KB for dbt), and
the two versions can't be joined directly without converting the type.

**Which output layout is easier for a consumer to read? Which one scales?**

The single dbt file is easier to read. The Spark folder scales better, because several tasks can write and read the
files in parallel. Both versions gave me the same result: 2,829 orders, 8,615 items and 50 users.

**What did this lab use instead of the dbt tests, and what would you need to write to get the same guarantees?**

Only `assert` checks on row counts and a manual search for duplicates. To get the same guarantees, I would have to
write each check myself in Spark: duplicates, missing values, allowed values, and links between tables.

**What plays the role of `ref()` and `source()` in the notebook?**

The Python variables and the order of the cells. `stg_orders` depends on `orders_typed`, which depends on
`orders_bronze`. But if I change a cell, I have to rerun the next ones myself, while dbt knows the order on its own.

**Why are the build times of dbt and Spark not directly comparable?**

`dbt build` took between 4 and 7 seconds. Spark does a lot more around it: it starts, downloads Iceberg, reads from
S3 and writes several files. With such a small dataset, this setup time is bigger than the real work.

**For each case, would you choose dbt with DuckDB or Spark?**

- 10 GB per day: dbt with DuckDB, one machine is enough.
- 10 TB per day: Spark, because the work has to be split over a cluster.
- Analysts who only know SQL: dbt, because everything is written in SQL.

## Exercises

**1. `birthdate_is_valid`**

I joined each user with the date of their first order and compared it with the birthdate. The DataFrame and SQL
versions give the same result: 45 valid and 5 invalid birthdates, like in the dbt lab.

**2. Python UDF and `pandas_udf`**

| version | average time |
| --- | --- |
| `regexp_extract` | 0.114 s |
| Python UDF | 0.127 s |
| `pandas_udf` | 0.154 s |

All three give the same zip codes. `regexp_extract` is the fastest because it doesn't need Python. With only 50
rows, the `pandas_udf` was the slowest, because it needs some time to start. It would be faster on a large dataset.
`%%timeit` only works in Jupyter, so I used `timeit`.

**3. Cache**

`stg_orders.count()` took 0.557 s without cache and 0.378 s with cache. The Storage information showed the two bronze
files in memory. It's faster because Spark doesn't read the files from S3 again.

**4. Time travel**

After the append, the Iceberg table had a second snapshot and 2,830 orders. With `VERSION AS OF`, I could still read
the old version with 2,829 orders. With the Parquet folder, I could add the order but not go back to the old version,
because Parquet keeps no history. I then removed the extra order with `rollback_to_snapshot` for Iceberg, and by
rewriting the folder for Parquet.
