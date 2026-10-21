# Lab 5: Spark RDD - Answers

## RDD creation and partitions

**What decides the number of partitions `textFile()` picks by default, for a file this size?**

`textFile()` cuts a file into blocks of several tens of MB, but it never goes below a minimum, which is 2 by default.
My files are only a few hundred KB, so they fit in a single block and this minimum is what decides. That's why I got
2 partitions for both `users.csv` and `orders.csv`.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**

`repartition()` shuffles all the data to build new partitions of the same size, so it works in both directions. I
used it to go from 2 to 8 partitions. `coalesce()` can only reduce the number of partitions: it merges neighbouring
partitions without moving the data around, so there is no shuffle, but the partitions may not be balanced.

## Lazy evaluation and lineage

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**

Yes. My two `count()` calls created two different stages, and both read the whole file again. Spark doesn't keep the
result of an action. Without cache, each action replays the full lineage from the start: read the file, then filter.

**At which point in this notebook would `.cache()` change that answer?**

Right after the `filter`, on `orders_data`, before the first `count()`. The first count would then store the filtered
lines in memory, and the second one would read them from there instead of reading the file again.

## The cost of no schema: the multiline address

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**

The CSV reader understands quotes: a line break inside quotes belongs to the value, so it reads one full record at a
time. `textFile()` doesn't know anything about CSV and cuts at every line break. In my case it gave 101 elements for
50 users: the header, then each user cut in two, with the end of the address alone on its line.

**What has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**

Each record has to fit on exactly one line, with no line break inside a value. Then a line break always means a new
record, whatever partition it lands in. `orders.csv` respects this, which is why splitting it line by line worked
fine, while `users.csv` doesn't.

## Manual parsing of orders

**What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**

`split(",")` would return 6 pieces instead of 5, and Python would raise a `ValueError` because it can't put 6 values
into 5 variables. The surprising part is when: because of lazy evaluation, nothing breaks at this cell. The error only
shows up at the first action that reaches the bad line, and the whole job fails.

**Where does the DataFrame API perform the equivalent check, and when?**

In the CSV reader, which applies the schema while it reads each row. By default a broken row gets NULL values, or the
read stops if the mode is `FAILFAST`. Column names are also checked before anything runs. I saw it myself: when I
wrote `groupBy("user_id")`, Spark refused straight away because the column is called `user_uuid` in the file.

## Key-value RDDs: total quantity per user

**What does `groupByKey` shuffle instead, and why is that more expensive?**

`groupByKey` sends every single pair through the shuffle, one per order, and only adds them up at the end, while
`reduceByKey` only sends one subtotal per user and per partition. With my data, `reduceByKey` wrote 2,513 bytes to the
shuffle and `groupByKey` 6,230 bytes, for exactly the same totals. On a big dataset, this means a lot more network
traffic, and all the values of one key also have to fit in memory together.

**For which aggregations would `groupByKey` be unavoidable?**

When I really need all the values of a key at the same time and can't build the result from partial results. A
median is a good example, or the list of every product a user bought.

## Manual deduplication

**What plays the role of `partitionBy()` here?**

The shuffle done by `reduceByKey`. It sends every version of the same `order_id` to the same partition, so they can be
compared. I could even see it in the lineage of `user_quantity`: `ShuffledRDD at partitionBy`.

**Which version states the intent more directly?**

The window function. `row_number() ... = 1` says the goal directly: one row per order, the latest one. With
`reduceByKey`, I have to read the `most_recent` function to understand what the code does. In my case, the result
`2829 2829` showed there was no duplicate in the generated orders.

## Joining orders with users

**What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**

Both RDDs would have to be split the same way, with the same partitioner and the same number of partitions, for
example by calling `partitionBy()` on both before the join. Each user and all their orders would then already sit on
the same partition. Since `users` is tiny, sending a full copy of it to every executor would also avoid the shuffle.

**Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching user?**

The order stays in the result, with `None` instead of the username. With `join()`, it just disappears without any
warning. I checked it in exercise 2: `leftOuterJoin()` found my fake order, while `join()` returned 2,829 rows
instead of 2,830.

**What is the risk of calling `.collect()` on `enriched` directly, on a much larger dataset?**

`collect()` brings every row back to the driver, which is a single process. With a big dataset, it would run out of
memory and crash. I would rather filter or aggregate first, use `take()` to look at a few rows, or write the result
to storage.

## Exercises

**1. Average quantity per user with `aggregateByKey`**

```python
user_stats = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, quantity: (acc[0] + 1, acc[1] + quantity),
    lambda acc_a, acc_b: (acc_a[0] + acc_b[0], acc_a[1] + acc_b[1]),
)
user_avg = user_stats.mapValues(lambda stats: round(stats[1] / stats[0], 2))
```

In one pass, I get the number of orders and the total quantity of each user. For example kayla51 has `(4, 12)`, so
an average of 3.0 items per order. The first function counts inside each partition, the second one merges the
partial results after the shuffle.

**2. Orphan order**

```python
orders_with_orphan = orders_rdd.union(sc.parallelize([fake_order]))
left_joined = orders_with_orphan.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
orphans = left_joined.filter(lambda kv: kv[1][1] is None)
```

I added one order with a `user_id` made of zeros. The copy has 2,830 orders, and the filter finds exactly 1 orphan,
my fake order, with `None` as username. The original `orders_rdd` still has 2,829 orders.

**3. DataFrame version**

```python
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True)
user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")
```

The lab writes `user_id`, but in the CSV the column is called `user_uuid`. Two lines replace about ten lines of RDD
code, and I didn't have to parse anything: Spark read `quantity` as an integer and `date` as a timestamp. In
`.explain()`, I can see that Spark only reads the 2 columns it needs and computes partial sums before the shuffle, the
same trick as `reduceByKey`, but chosen on its own. The RDD's `.toDebugString()` only shows a chain of `PythonRDD`:
Spark runs my functions without knowing what they do, so it can't optimise them.

**4. Cache**

| | with cache | without cache |
| --- | --- | --- |
| `orders_rdd.count()` | 0.359 s | 0.124 s |
| `user_quantity.count()` | 0.197 s | 0.376 s |

The Storage tab only listed `orders_rdd`, with its 2 partitions in memory (172 KB). The version without cache never
showed up there. The cache made `user_quantity` faster, but not the first count. Honestly, with a 330 KB file,
starting the tasks takes longer than reading the data, so these timings are mostly noise. I would expect the cache to
make a real difference only on a much bigger file, where each action without cache reads everything again.

## Note

The Spark UI (`localhost:4040`) didn't open properly through VS Code on Onyxia, so I read the same information from
the Spark UI REST API (`/api/v1/applications/<id>/stages` and `/storage/rdd`) with `curl` and `jq`.
