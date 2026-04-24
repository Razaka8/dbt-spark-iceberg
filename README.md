# dbt-spark-iceberg

## > dbt debug
```bash
dbt debug
14:24:24  Running with dbt=1.11.7
14:24:24  dbt version: 1.11.7
14:24:24  python version: 3.10.2
14:24:24  python path: path\projets\projet3_dbt_data_lakehouse\.dbt_env\Scripts\python.exe
14:24:24  os info: Windows-10-10.0.26100-SP0
14:24:33  Using profiles dir at path\projets\projet3_dbt_data_lakehouse\dbt-spark-iceberg\dbt-project\spark_iceberg
14:24:33  Using profiles.yml file at path\projets\projet3_dbt_data_lakehouse\dbt-spark-iceberg\dbt-project\spark_iceberg\profiles.yml
14:24:33  Using dbt_project.yml file at path\projets\projet3_dbt_data_lakehouse\dbt-spark-iceberg\dbt-project\spark_iceberg\dbt_project.yml
14:24:33  adapter type: spark
14:24:33  adapter version: 1.10.1
14:24:33  Configuration:
14:24:33    profiles.yml file [OK found and valid]
14:24:33    dbt_project.yml file [OK found and valid]
14:24:33  Required dependencies:
14:24:34   - git [OK found]

14:24:34  Connection:
14:24:34    host: 192.168.70.2
14:24:34    port: 10000
14:24:34    cluster: None
14:24:34    endpoint: None
14:24:34    schema: analytics
14:24:34    organization: 0
14:24:34  Registered adapter: spark=1.10.1
14:25:13    Connection test: [OK connection ok]

14:25:13  All checks passed!
```

## > dbt run

```bash
$ dbt run
14:25:37  Running with dbt=1.11.7
14:25:39  Registered adapter: spark=1.10.1
14:25:41  Unable to do partial parsing because a project dependency has been added
14:25:56  Found 2 models, 4 data tests, 504 macros
14:25:57
14:25:57  Concurrency: 1 threads (target='dev')
14:25:57
14:26:27  1 of 2 START sql table model analytics.my_first_dbt_model ...................... [RUN]
14:27:12  1 of 2 OK created sql table model analytics.my_first_dbt_model ................. [OK in 44.80s]
14:27:12  2 of 2 START sql view model analytics.my_second_dbt_model ...................... [RUN]
14:27:23  2 of 2 OK created sql view model analytics.my_second_dbt_model ................. [OK in 10.78s]
14:27:24
14:27:24  Finished running 1 table model, 1 view model in 0 hours 1 minutes and 27.10 seconds (87.10s).
14:27:24
14:27:24  Completed successfully
14:27:24
14:27:24  Done. PASS=2 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=2
```

## PySpark

```python
# spark-sql.py

import sys

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
        .appName("audit tables and views in dbt project")
        .remote("sc://192.168.70.2:15002")
        .getOrCreate()
)

queries = """show namespaces
show tables in analytics
show views in analytics"""

for query in queries.split("\n"):
    print(f"\n> {query}")
    spark.sql(f" {query}").show(truncate=False)


spark.stop()
```

```bash
$ python spark-sql.py

> show namespaces
+------------------+
|namespace         |
+------------------+
|analytics         |
|default           |
+------------------+


> show tables in analytics
+---------+------------------+-----------+
|namespace|tableName         |isTemporary|
+---------+------------------+-----------+
|analytics|my_first_dbt_model|false      |
+---------+------------------+-----------+


> show views in analytics
+---------+-------------------+-----------+
|namespace|viewName           |isTemporary|
+---------+-------------------+-----------+
|analytics|my_second_dbt_model|false      |
+---------+-------------------+-----------+
```