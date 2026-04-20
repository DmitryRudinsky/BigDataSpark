# BigDataSpark — запуск

## 0) Podman Machine (macOS)

```bash
podman machine init
podman machine start
podman info
```

## 1) Поднять окружение

```bash
cd BigDataSpark
podman compose up -d
```

## 2) Проверить загрузку исходных данных в PostgreSQL

```bash
podman exec bigdataspark_postgres psql -U admin -d bigdata -tAc "select count(*) from mock_data;"
```

## 3) Собрать jar

```bash
cd BigDataSpark/spark-jobs
./gradlew clean shadowJar
```

```bash
ls -la build/libs
```

## 4) Проверить jar внутри Spark-контейнера

```bash
podman exec bigdataspark_spark bash -lc "ls -la /opt/spark/work-dir/jobs"
```

## 5) Запустить Job1 (PostgreSQL → dw.*)

```bash
podman exec bigdataspark_spark bash -lc \
"/opt/spark/bin/spark-submit --master local[*] \
 --class ru.mai.highload.Job1_LoadStarSchema \
 /opt/spark/work-dir/jobs/bigdataspark-spark-jobs-1.0.0-SNAPSHOT-all.jar"
```

```bash
podman exec bigdataspark_postgres psql -U admin -d bigdata -c "\dt dw.*"
```

```bash
podman exec bigdataspark_postgres psql -U admin -d bigdata -tAc "select count(*) from dw.fact_sales;"
```

## 6) Запустить Job2 (dw.* → ClickHouse + Cassandra + MongoDB)

```bash
podman exec bigdataspark_spark bash -lc \
"/opt/spark/bin/spark-submit --master local[*] \
 --class ru.mai.highload.Job2_BuildMarts \
 /opt/spark/work-dir/jobs/bigdataspark-spark-jobs-1.0.0-SNAPSHOT-all.jar"
```

Отключить Cassandra/Mongo (опционально):

```bash
podman exec bigdataspark_spark bash -lc \
"/opt/spark/bin/spark-submit --master local[*] \
 --class ru.mai.highload.Job2_BuildMarts \
 /opt/spark/work-dir/jobs/bigdataspark-spark-jobs-1.0.0-SNAPSHOT-all.jar \
 --enableCassandra false --enableMongo false"
```

## 7) Быстрые проверки витрин

```bash
podman exec bigdataspark_clickhouse clickhouse-client -q "\
SELECT 'mart_product_sales', count(*) FROM reports.mart_product_sales \
UNION ALL SELECT 'mart_customer_sales', count(*) FROM reports.mart_customer_sales \
UNION ALL SELECT 'mart_time_sales', count(*) FROM reports.mart_time_sales \
UNION ALL SELECT 'mart_store_sales', count(*) FROM reports.mart_store_sales \
UNION ALL SELECT 'mart_supplier_sales', count(*) FROM reports.mart_supplier_sales \
UNION ALL SELECT 'mart_product_quality', count(*) FROM reports.mart_product_quality"
```

```bash
podman exec bigdataspark_cassandra bash -lc \
"/opt/cassandra/bin/cqlsh -e \"SELECT count(*) FROM reports.mart_product_sales;\""
```

```bash
podman exec bigdataspark_mongodb mongosh --quiet --eval "\
db.getSiblingDB('reports').mart_product_sales.countDocuments()"
```

## 8) Остановка окружения

```bash
cd BigDataSpark
podman compose down
```

```bash
podman compose down -v
```

## 9) Пример работы:

➜  spark-jobs git:(main) ✗ podman exec bigdataspark_postgres psql -U admin -d bigdata -tAc "select count(*) from mock_data;"
10000
➜  spark-jobs git:(main) ✗ podman exec bigdataspark_postgres psql -U admin -d bigdata -c "\dt dw.*"
           List of relations
 Schema |     Name     | Type  | Owner 
--------+--------------+-------+-------
 dw     | dim_customer | table | admin
 dw     | dim_date     | table | admin
 dw     | dim_product  | table | admin
 dw     | dim_seller   | table | admin
 dw     | dim_store    | table | admin
 dw     | dim_supplier | table | admin
 dw     | fact_sales   | table | admin
(7 rows)

➜  spark-jobs git:(main) ✗ podman exec bigdataspark_postgres psql -U admin -d bigdata -tAc "select count(*) from dw.fact_sales;"
10000
➜  spark-jobs git:(main) ✗ podman exec bigdataspark_clickhouse clickhouse-client -q "\
SELECT 'mart_product_sales', count(*) FROM reports.mart_product_sales \
UNION ALL SELECT 'mart_customer_sales', count(*) FROM reports.mart_customer_sales \
UNION ALL SELECT 'mart_time_sales', count(*) FROM reports.mart_time_sales \
UNION ALL SELECT 'mart_store_sales', count(*) FROM reports.mart_store_sales \
UNION ALL SELECT 'mart_supplier_sales', count(*) FROM reports.mart_supplier_sales \
UNION ALL SELECT 'mart_product_quality', count(*) FROM reports.mart_product_quality"
mart_time_sales 12
mart_store_sales        10000
mart_product_quality    1000
mart_supplier_sales     10000
mart_customer_sales     1000
mart_product_sales      1000
➜  spark-jobs git:(main) ✗ podman exec bigdataspark_clickhouse clickhouse-client -q "\
SELECT product_id, product_name, total_units_sold, total_revenue \
FROM reports.mart_product_sales \
ORDER BY total_units_sold DESC \
LIMIT 10"
562     Cat Toy 84      2791.52
963     Cat Toy 84      2116.63
692     Dog Food        80      2964.14
673     Bird Cage       80      2113.29
392     Bird Cage       78      2809.37
622     Cat Toy 77      2185.49
699     Dog Food        77      2691.13
790     Cat Toy 77      2325.62
624     Cat Toy 77      2621.96
381     Dog Food        76      3086.86
➜  spark-jobs git:(main) ✗ podman exec bigdataspark_cassandra bash -lc \
"/opt/cassandra/bin/cqlsh -e \"SELECT count(*) FROM reports.mart_product_sales;\""

 count
-------
  1000

(1 rows)

Warnings :
Aggregation query used without partition key

➜  spark-jobs git:(main) ✗ podman exec bigdataspark_mongodb mongosh --quiet --eval "\
db.getSiblingDB('reports').mart_product_sales.find().limit(5).toArray()"
[
  {
    _id: ObjectId('69e6b78860aaa47d66ebe739'),
    product_id: 269,
    product_name: 'Bird Cage',
    product_category: 'Cage',
    total_revenue: 3682.52,
    total_units_sold: Long('65'),
    sales_count: Long('10'),
    product_rating: Decimal128('1.5'),
    product_reviews: 524,
    rank_by_units: 131,
    rank_by_revenue: 4,
    category_total_revenue: 843903.83
  },
  {
    _id: ObjectId('69e6b78860aaa47d66ebe73a'),
    product_id: 384,
    product_name: 'Bird Cage',
    product_category: 'Cage',
    total_revenue: 3478.9,
    total_units_sold: Long('45'),
    sales_count: Long('10'),
    product_rating: Decimal128('2.9'),
    product_reviews: 88,
    rank_by_units: 848,
    rank_by_revenue: 12,
    category_total_revenue: 843903.83
  },
  {
    _id: ObjectId('69e6b78860aaa47d66ebe73b'),
    product_id: 251,
    product_name: 'Cat Toy',
    product_category: 'Cage',
    total_revenue: 3453.29,
    total_units_sold: Long('51'),
    sales_count: Long('10'),
    product_rating: Decimal128('2.8'),
    product_reviews: 990,
    rank_by_units: 631,
    rank_by_revenue: 14,
    category_total_revenue: 843903.83
  },
  {
    _id: ObjectId('69e6b78860aaa47d66ebe73c'),
    product_id: 606,
    product_name: 'Dog Food',
    product_category: 'Cage',
    total_revenue: 3441.36,
    total_units_sold: Long('60'),
    sales_count: Long('10'),
    product_rating: Decimal128('3.7'),
    product_reviews: 80,
    rank_by_units: 260,
    rank_by_revenue: 16,
    category_total_revenue: 843903.83
  },
  {
    _id: ObjectId('69e6b78860aaa47d66ebe73d'),
    product_id: 765,
    product_name: 'Bird Cage',
    product_category: 'Cage',
    total_revenue: 3418.49,
    total_units_sold: Long('57'),
    sales_count: Long('10'),
    product_rating: Decimal128('1.5'),
    product_reviews: 569,
    rank_by_units: 363,
    rank_by_revenue: 18,
    category_total_revenue: 843903.83
  }
]