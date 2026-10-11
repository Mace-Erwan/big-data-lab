# Lab 3: DuckDB - Answers

## S3 configuration

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**

The credentials are written in clear text, so any program running as my user can read them, and the file could end up
in a backup or a Git commit by mistake. On Onyxia it's not a big deal: the credentials expire after a few hours and the
service runs in my own isolated space.

**How would you restrict the access to S3 for a Kubernetes Job?**

I would create a Secret used only by that Job, with its own service account, and use RBAC so other pods can't read it.
The credentials should only give access to what the Job needs, ideally read-only on one prefix.

## Query the bronze layer

**Why does `approx_unique` return 40 and 3167?**

Because it doesn't count exactly. It uses an estimation algorithm, which is very fast on big data but has a margin of
error. To get the real numbers, you need `count(DISTINCT ...)`.

**Which issue may occur with a large file whose first rows are not representative?**

DuckDB can guess the wrong type. If a column only has numbers at the beginning but text later, it will be read as an
integer and the query will fail. Giving the schema explicitly, or using Parquet, avoids this.

## Parquet export

**Why is the `uuid` column barely compressed?**

Every uuid is unique and random, so there's nothing repeated to compress.

**Why is the compressed size of some columns larger than their uncompressed size?**

These columns are already very compact, so the compression can't gain anything and just adds a few bytes of its own.

## Hive partitioning

**Why is partitioning by `uuid` a bad idea?**

We would get one tiny file per order. Object storage is slow with lots of small files: every file is a separate
request and listing them takes a long time.

**Which partition column would you choose for orders growing every day?**

The order date, for example one partition per day. Most queries filter on a period, and each new day just adds a new
partition.

## CSV vs. Parquet at scale

**How many row groups does the file contain, and how many are skipped by `date >= '2100-01-01'`?**

My file has 3 row groups with dates from 2020 to 2048, so all 3 are skipped. DuckDB only read the footer (16 KiB).

**What would happen if the orders were shuffled?**

Each row group would contain dates from the whole period, so none of them could be skipped and DuckDB would have to
read everything.

**Which part of the difference is due to the network, which part to the parsing of the CSV file?**

The CSV query downloaded 28 MiB against 200 KiB for Parquet, so most of the gap comes from the network. The rest comes
from parsing: the CSV is text that has to be split and converted, while Parquet is already typed and only the needed
columns are read.
