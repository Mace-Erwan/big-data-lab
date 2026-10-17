# Lab 4: dbt - Answers

## Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**

Since the raw files never change, a bug in a model is not a disaster. I just fix the SQL and run `dbt build` again,
and the silver layer is rebuilt from the original data. I actually did this during the lab: I deleted the DuckDB
database and `dbt build` recreated everything from the bronze files.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?**

I would store them in Parquet, partitioned by date, for example one folder per day. In the DuckDB lab, the same query
downloaded 28 MB from a CSV file but only 200 KB from a Parquet file, because Parquet only reads the needed columns.
With a partition per day, a query on one week would also skip all the other days.

## Build

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**

A view doesn't store any data, only the query. So every time I query `stg_users`, DuckDB reads the CSV from S3 and
does the whole transformation again. It's a good choice when the data is small or not queried very often, because it
is always up to date and takes no space. When the data gets bigger, or a dashboard queries it all day, a table is
better.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**

As text, a UUID has 36 characters (32 hex digits and 4 dashes), so 36 bytes, more than twice the size. I could see
the difference: my `stg_orders` Parquet file is 72 KB, while the Parquet file of the same orders in the DuckDB lab,
with text identifiers, was 130 KB.

## Data tests

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**

It depends on what the dashboard uses. For the age, the birthdate is the actual data shown, so I would replace the
wrong dates with NULL and leave these customers out of the age statistics. Showing someone born in 2025 who ordered in
2020 would make no sense. For sales per region, the birthdate isn't used at all, so removing those orders would mean
losing real sales. There I would keep them and just monitor the problem with a warning.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which reference data would it require?**

A test that checks the first digits of each zip code against the ranges allowed for the state. In the US, the first
three digits show the area, a bit like the first two digits of a French postal code show the département (75 for
Paris). It needs a reference table of zip ranges per state, ideally from the USPS. I built this test in exercise 4.

## Exercises

**Exercise 4: invalid zip codes**

I added a small `states` seed with the allowed zip prefixes for each state, and a singular test that compares every
user with it. 49 users out of 50 fail, which makes sense since Faker picks zip codes at random. I set the test to
`warn`, because the problem comes from the source data and can't be fixed in the pipeline. The seed also fixed the
`relationships` test on `state`, which was disabled from the start because `ref('states')` didn't exist.

- **Where should the reference data come from?** From an official source like the USPS or the US Census Bureau, and
  we should note where and when it was taken. Mine is a simplified version, good enough for the exercise.
- **Should it be stored as a dbt seed?** Yes, here it's small (74 rows) and rarely changes, so a CSV in Git is
  perfect. A big reference updated every day would be better loaded as a source.
- **Should the validation happen in the silver layer?** Yes, the silver layer is where data is cleaned and checked,
  so problems are caught before they reach the dashboards.
- **What happens when the reference data changes?** The seed has to be updated and loaded again with `dbt seed`,
  then the tests run again. Some addresses could become valid or invalid, which is why keeping the seed in Git is
  useful: we can see what changed and when.

**Exercise 5: views vs tables**

When I switched to tables, the models became `BASE TABLE` and the database file grew to 1.1 MB, because the data was
now stored inside it.

- **Where is the data stored?** With views, it stays in the CSV files on S3. With tables, it's copied into the DuckDB file.
- **When is the transformation executed?** For a view, at every query. For a table, only once during `dbt build`.
- **What happens when the CSV changes?** A view sees the change right away. A table keeps the old data until the next build.
- **Which approach requires more storage?** Tables, since they hold a copy of the data.
- **Which one avoids re-reading the source files?** Tables. That's the real trade-off: views are always fresh but
  slower, tables are fast but can be out of date.
