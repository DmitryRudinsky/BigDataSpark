package ru.mai.highload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Locale;
import java.util.Map;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Job2_BuildMarts {
    public static void main(String[] args) {
        Locale.setDefault(Locale.ROOT);

        String pgUrl = getArg(args, "--pgUrl", envOrDefault("PG_URL", "jdbc:postgresql://postgres:5432/bigdata"));
        String pgUser = getArg(args, "--pgUser", envOrDefault("PG_USER", "admin"));
        String pgPassword = getArg(args, "--pgPassword", envOrDefault("PG_PASSWORD", "admin"));

        String chUrl = getArg(args, "--chUrl", envOrDefault("CH_URL", "jdbc:clickhouse://clickhouse:8123/default"));
        String chUser = getArg(args, "--chUser", envOrDefault("CH_USER", "admin"));
        String chPassword = getArg(args, "--chPassword", envOrDefault("CH_PASSWORD", "admin"));
        String chDb = getArg(args, "--chDb", envOrDefault("CH_DB", "reports"));

        boolean enableCassandra = Boolean.parseBoolean(getArg(args, "--enableCassandra", envOrDefault("ENABLE_CASSANDRA", "true")));
        String cassandraHost = getArg(args, "--cassandraHost", envOrDefault("CASSANDRA_HOST", "cassandra"));
        int cassandraPort = Integer.parseInt(getArg(args, "--cassandraPort", envOrDefault("CASSANDRA_PORT", "9042")));
        String cassandraKeyspace = getArg(args, "--cassandraKeyspace", envOrDefault("CASSANDRA_KEYSPACE", "reports"));

        boolean enableMongo = Boolean.parseBoolean(getArg(args, "--enableMongo", envOrDefault("ENABLE_MONGO", "true")));
        String mongoUri = getArg(args, "--mongoUri", envOrDefault("MONGO_URI", "mongodb://mongodb:27017"));
        String mongoDb = getArg(args, "--mongoDb", envOrDefault("MONGO_DB", "reports"));

        SparkSession spark = SparkSession.builder()
                .appName("Job2_BuildMarts")
                .getOrCreate();

        Dataset<Row> factSales = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.fact_sales");
        Dataset<Row> dimProduct = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.dim_product");
        Dataset<Row> dimCustomer = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.dim_customer");
        Dataset<Row> dimStore = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.dim_store");
        Dataset<Row> dimSupplier = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.dim_supplier");
        Dataset<Row> dimDate = readJdbcTable(spark, pgUrl, pgUser, pgPassword, "dw.dim_date");

        Dataset<Row> martProductSales = buildMartProductSales(factSales, dimProduct);
        Dataset<Row> martCustomerSales = buildMartCustomerSales(factSales, dimCustomer);
        Dataset<Row> martTimeSales = buildMartTimeSales(factSales, dimDate);
        Dataset<Row> martStoreSales = buildMartStoreSales(factSales, dimStore);
        Dataset<Row> martSupplierSales = buildMartSupplierSales(factSales, dimSupplier, dimProduct);
        Dataset<Row> martProductQuality = buildMartProductQuality(factSales, dimProduct);

        Map<String, Dataset<Row>> marts = Map.of(
                "mart_product_sales", martProductSales,
                "mart_customer_sales", martCustomerSales,
                "mart_time_sales", martTimeSales,
                "mart_store_sales", martStoreSales,
                "mart_supplier_sales", martSupplierSales,
                "mart_product_quality", martProductQuality
        );

        ensureClickHouseDatabase(chUrl, chUser, chPassword, chDb);
        ensureClickHouseTables(chUrl, chUser, chPassword, chDb);
        for (Map.Entry<String, Dataset<Row>> e : marts.entrySet()) {
            String name = e.getKey();
            Dataset<Row> df = e.getValue();
            String fullTable = chDb + "." + name;
            truncateClickHouseTable(chUrl, chUser, chPassword, fullTable);
            writeClickHouseJdbc(df, chUrl, chUser, chPassword, fullTable);
            System.out.println("ClickHouse " + name + " written to " + fullTable);
        }

        if (enableCassandra) {
            ensureCassandraSchema(cassandraHost, cassandraPort, cassandraKeyspace);
            for (Map.Entry<String, Dataset<Row>> e : marts.entrySet()) {
                writeToCassandra(e.getValue(), cassandraHost, cassandraPort, cassandraKeyspace, e.getKey());
                System.out.println("Cassandra " + e.getKey() + " written to " + cassandraKeyspace + "." + e.getKey());
            }
        }

        if (enableMongo) {
            for (Map.Entry<String, Dataset<Row>> e : marts.entrySet()) {
                writeToMongo(e.getValue(), mongoUri, mongoDb, e.getKey());
                System.out.println("MongoDB " + e.getKey() + " written to " + mongoDb + "." + e.getKey());
            }
        }

        spark.stop();
    }

    private static Dataset<Row> buildMartProductSales(Dataset<Row> factSales, Dataset<Row> dimProduct) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> p = dimProduct.alias("p");
        Dataset<Row> salesWithProduct = f
                .join(p, col("f.product_id").equalTo(col("p.product_id")), "left");

        Dataset<Row> agg = salesWithProduct
                .groupBy(
                        col("f.product_id").alias("product_id"),
                        col("p.name").alias("product_name"),
                        col("p.category").alias("product_category"),
                        col("p.rating").alias("product_rating"),
                        col("p.reviews").alias("product_reviews")
                )
                .agg(
                        sum(col("f.total_price")).alias("total_revenue"),
                        sum(col("f.sale_quantity")).alias("total_units_sold"),
                        count(lit(1)).alias("sales_count")
                );

        WindowSpec byUnits = Window.orderBy(col("total_units_sold").desc_nulls_last(), col("total_revenue").desc_nulls_last());
        WindowSpec byRevenue = Window.orderBy(col("total_revenue").desc_nulls_last(), col("total_units_sold").desc_nulls_last());
        WindowSpec byCategoryRevenue = Window.partitionBy(col("product_category"));

        return agg
                .withColumn("rank_by_units", dense_rank().over(byUnits))
                .withColumn("rank_by_revenue", dense_rank().over(byRevenue))
                .withColumn("category_total_revenue", sum(col("total_revenue")).over(byCategoryRevenue))
                .select(
                        col("product_id"),
                        col("product_name"),
                        col("product_category"),
                        col("total_revenue"),
                        col("total_units_sold"),
                        col("sales_count"),
                        col("product_rating"),
                        col("product_reviews"),
                        col("rank_by_units"),
                        col("rank_by_revenue"),
                        col("category_total_revenue")
                );
    }

    private static Dataset<Row> buildMartCustomerSales(Dataset<Row> factSales, Dataset<Row> dimCustomer) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> c = dimCustomer.alias("c");
        Dataset<Row> joined = f.join(c, col("f.customer_id").equalTo(col("c.customer_id")), "left");

        Dataset<Row> agg = joined
                .groupBy(
                        col("f.customer_id").alias("customer_id"),
                        col("c.country").alias("customer_country")
                )
                .agg(
                        sum(col("f.total_price")).alias("total_spent"),
                        count(lit(1)).alias("orders_count")
                )
                .withColumn("avg_check", bround(col("total_spent").divide(col("orders_count")), 2));

        WindowSpec bySpent = Window.orderBy(col("total_spent").desc_nulls_last(), col("orders_count").desc_nulls_last());
        return agg
                .withColumn("rank_by_total_spent", dense_rank().over(bySpent))
                .select(
                        col("customer_id"),
                        col("customer_country"),
                        col("total_spent"),
                        col("orders_count"),
                        col("avg_check"),
                        col("rank_by_total_spent")
                );
    }

    private static Dataset<Row> buildMartTimeSales(Dataset<Row> factSales, Dataset<Row> dimDate) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> d = dimDate.alias("d");
        Dataset<Row> joined = f.join(d, col("f.date_key").equalTo(col("d.date_key")), "left");

        Dataset<Row> agg = joined
                .groupBy(col("d.year").alias("year"), col("d.month").alias("month"))
                .agg(
                        sum(col("f.total_price")).alias("total_revenue"),
                        count(lit(1)).alias("orders_count")
                )
                .withColumn("avg_order_value", bround(col("total_revenue").divide(col("orders_count")), 2))
                .select("year", "month", "total_revenue", "orders_count", "avg_order_value");

        return agg;
    }

    private static Dataset<Row> buildMartStoreSales(Dataset<Row> factSales, Dataset<Row> dimStore) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> s = dimStore.alias("s");
        Dataset<Row> joined = f.join(s, col("f.store_key").equalTo(col("s.store_key")), "left");

        Dataset<Row> agg = joined
                .groupBy(
                        col("f.store_key").alias("store_key"),
                        col("s.name").alias("store_name"),
                        col("s.city").alias("store_city"),
                        col("s.country").alias("store_country")
                )
                .agg(
                        sum(col("f.total_price")).alias("total_revenue"),
                        count(lit(1)).alias("orders_count")
                )
                .withColumn("avg_check", bround(col("total_revenue").divide(col("orders_count")), 2));

        WindowSpec byRevenue = Window.orderBy(col("total_revenue").desc_nulls_last(), col("orders_count").desc_nulls_last());
        return agg
                .withColumn("rank_by_revenue", dense_rank().over(byRevenue))
                .select(
                        col("store_key"),
                        col("store_name"),
                        col("store_city"),
                        col("store_country"),
                        col("total_revenue"),
                        col("orders_count"),
                        col("avg_check"),
                        col("rank_by_revenue")
                );
    }

    private static Dataset<Row> buildMartSupplierSales(Dataset<Row> factSales, Dataset<Row> dimSupplier, Dataset<Row> dimProduct) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> sup = dimSupplier.alias("sup");
        Dataset<Row> p = dimProduct.alias("p");

        Dataset<Row> joined = f
                .join(sup, col("f.supplier_key").equalTo(col("sup.supplier_key")), "left")
                .join(p, col("f.product_id").equalTo(col("p.product_id")), "left");

        Dataset<Row> agg = joined
                .groupBy(
                        col("f.supplier_key").alias("supplier_key"),
                        col("sup.name").alias("supplier_name"),
                        col("sup.country").alias("supplier_country")
                )
                .agg(
                        sum(col("f.total_price")).alias("total_revenue"),
                        count(lit(1)).alias("orders_count"),
                        bround(avg(col("p.price")), 2).alias("avg_product_price")
                );

        WindowSpec byRevenue = Window.orderBy(col("total_revenue").desc_nulls_last(), col("orders_count").desc_nulls_last());
        return agg
                .withColumn("rank_by_revenue", dense_rank().over(byRevenue))
                .select(
                        col("supplier_key"),
                        col("supplier_name"),
                        col("supplier_country"),
                        col("total_revenue"),
                        col("orders_count"),
                        col("avg_product_price"),
                        col("rank_by_revenue")
                );
    }

    private static Dataset<Row> buildMartProductQuality(Dataset<Row> factSales, Dataset<Row> dimProduct) {
        Dataset<Row> f = factSales.alias("f");
        Dataset<Row> p = dimProduct.alias("p");
        Dataset<Row> joined = f.join(p, col("f.product_id").equalTo(col("p.product_id")), "left");

        return joined
                .groupBy(
                        col("f.product_id").alias("product_id"),
                        col("p.name").alias("product_name"),
                        col("p.rating").alias("avg_rating"),
                        col("p.reviews").alias("product_reviews")
                )
                .agg(
                        sum(col("f.sale_quantity")).alias("total_units_sold"),
                        sum(col("f.total_price")).alias("total_revenue")
                )
                .select("product_id", "product_name", "avg_rating", "product_reviews", "total_units_sold", "total_revenue");
    }

    private static Dataset<Row> readJdbcTable(SparkSession spark, String jdbcUrl, String user, String password, String table) {
        return spark.read()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .load();
    }

    private static void writeClickHouseJdbc(Dataset<Row> df, String jdbcUrl, String user, String password, String table) {
        df.write()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .mode("append")
                .save();
    }

    private static void ensureClickHouseDatabase(String jdbcUrl, String user, String password, String db) {
        loadClickHouseDriver();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement st = conn.createStatement()) {
            st.execute("CREATE DATABASE IF NOT EXISTS " + db);
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure ClickHouse database: " + db, e);
        }
    }

    private static void ensureClickHouseTables(String jdbcUrl, String user, String password, String db) {
        loadClickHouseDriver();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement st = conn.createStatement()) {
            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_product_sales (" +
                            "product_id Int32," +
                            "product_name String," +
                            "product_category String," +
                            "total_revenue Decimal(12,2)," +
                            "total_units_sold Int64," +
                            "sales_count Int64," +
                            "product_rating Nullable(Decimal(3,1))," +
                            "product_reviews Nullable(Int32)," +
                            "rank_by_units Int32," +
                            "rank_by_revenue Int32," +
                            "category_total_revenue Decimal(12,2)" +
                            ") ENGINE = MergeTree ORDER BY (product_category, product_id)"
            );

            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_customer_sales (" +
                            "customer_id Int32," +
                            "customer_country String," +
                            "total_spent Decimal(12,2)," +
                            "orders_count Int64," +
                            "avg_check Decimal(12,2)," +
                            "rank_by_total_spent Int32" +
                            ") ENGINE = MergeTree ORDER BY (customer_country, customer_id)"
            );

            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_time_sales (" +
                            "year Int32," +
                            "month Int32," +
                            "total_revenue Decimal(12,2)," +
                            "orders_count Int64," +
                            "avg_order_value Decimal(12,2)" +
                            ") ENGINE = MergeTree ORDER BY (year, month)"
            );

            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_store_sales (" +
                            "store_key String," +
                            "store_name String," +
                            "store_city String," +
                            "store_country String," +
                            "total_revenue Decimal(12,2)," +
                            "orders_count Int64," +
                            "avg_check Decimal(12,2)," +
                            "rank_by_revenue Int32" +
                            ") ENGINE = MergeTree ORDER BY (store_country, store_city, store_key)"
            );

            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_supplier_sales (" +
                            "supplier_key String," +
                            "supplier_name String," +
                            "supplier_country String," +
                            "total_revenue Decimal(12,2)," +
                            "orders_count Int64," +
                            "avg_product_price Decimal(12,2)," +
                            "rank_by_revenue Int32" +
                            ") ENGINE = MergeTree ORDER BY (supplier_country, supplier_key)"
            );

            st.execute(
                    "CREATE TABLE IF NOT EXISTS " + db + ".mart_product_quality (" +
                            "product_id Int32," +
                            "product_name String," +
                            "avg_rating Nullable(Decimal(3,1))," +
                            "product_reviews Nullable(Int32)," +
                            "total_units_sold Int64," +
                            "total_revenue Decimal(12,2)" +
                            ") ENGINE = MergeTree ORDER BY (product_id)"
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure ClickHouse tables in database: " + db, e);
        }
    }

    private static void truncateClickHouseTable(String jdbcUrl, String user, String password, String table) {
        loadClickHouseDriver();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement st = conn.createStatement()) {
            st.execute("TRUNCATE TABLE " + table);
        } catch (Exception e) {
            throw new RuntimeException("Failed to truncate ClickHouse table: " + table, e);
        }
    }

    private static void loadClickHouseDriver() {
        try {
            Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("ClickHouse JDBC driver is missing from classpath", e);
        }
    }

    private static void ensureCassandraSchema(String host, int port, String keyspace) {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new java.net.InetSocketAddress(host, port))
                .withLocalDatacenter("datacenter1")
                .build()) {
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_product_sales (" +
                    "product_id int PRIMARY KEY," +
                    "product_name text," +
                    "product_category text," +
                    "total_revenue double," +
                    "total_units_sold bigint," +
                    "sales_count bigint," +
                    "product_rating double," +
                    "product_reviews int," +
                    "rank_by_units int," +
                    "rank_by_revenue int," +
                    "category_total_revenue double)");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_customer_sales (" +
                    "customer_id int PRIMARY KEY," +
                    "customer_country text," +
                    "total_spent double," +
                    "orders_count bigint," +
                    "avg_check double," +
                    "rank_by_total_spent int)");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_time_sales (" +
                    "year int," +
                    "month int," +
                    "total_revenue double," +
                    "orders_count bigint," +
                    "avg_order_value double," +
                    "PRIMARY KEY ((year), month))");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_store_sales (" +
                    "store_key text PRIMARY KEY," +
                    "store_name text," +
                    "store_city text," +
                    "store_country text," +
                    "total_revenue double," +
                    "orders_count bigint," +
                    "avg_check double," +
                    "rank_by_revenue int)");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_supplier_sales (" +
                    "supplier_key text PRIMARY KEY," +
                    "supplier_name text," +
                    "supplier_country text," +
                    "total_revenue double," +
                    "orders_count bigint," +
                    "avg_product_price double," +
                    "rank_by_revenue int)");

            session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mart_product_quality (" +
                    "product_id int PRIMARY KEY," +
                    "product_name text," +
                    "avg_rating double," +
                    "product_reviews int," +
                    "total_units_sold bigint," +
                    "total_revenue double)");
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure Cassandra schema in keyspace: " + keyspace, e);
        }
    }

    private static void writeToCassandra(Dataset<Row> df, String host, int port, String keyspace, String table) {
        Dataset<Row> normalized = normalizeForCassandra(df);
        normalized.write()
                .format("org.apache.spark.sql.cassandra")
                .option("spark.cassandra.connection.host", host)
                .option("spark.cassandra.connection.port", String.valueOf(port))
                .option("keyspace", keyspace)
                .option("table", table)
                .mode("append")
                .save();
    }

    private static Dataset<Row> normalizeForCassandra(Dataset<Row> df) {
        Dataset<Row> out = df;
        for (String c : df.columns()) {
            if (c.toLowerCase(Locale.ROOT).contains("revenue") ||
                    c.toLowerCase(Locale.ROOT).contains("spent") ||
                    c.toLowerCase(Locale.ROOT).contains("check") ||
                    c.toLowerCase(Locale.ROOT).contains("price")) {
                out = out.withColumn(c, col(c).cast("double"));
            }
            if (c.toLowerCase(Locale.ROOT).contains("count") ||
                    c.toLowerCase(Locale.ROOT).contains("units_sold") ||
                    c.toLowerCase(Locale.ROOT).contains("orders_count")) {
                out = out.withColumn(c, col(c).cast("long"));
            }
            if (c.toLowerCase(Locale.ROOT).contains("rating")) {
                out = out.withColumn(c, col(c).cast("double"));
            }
            if (c.toLowerCase(Locale.ROOT).contains("reviews")) {
                out = out.withColumn(c, col(c).cast("int"));
            }
            if (c.startsWith("rank_")) {
                out = out.withColumn(c, col(c).cast("int"));
            }
        }
        return out;
    }

    private static void writeToMongo(Dataset<Row> df, String mongoUri, String db, String collection) {
        Dataset<Row> normalized = normalizeForMongo(df);
        normalized.write()
                .format("mongodb")
                .mode("overwrite")
                .option("spark.mongodb.write.connection.uri", mongoUri)
                .option("database", db)
                .option("collection", collection)
                .save();
    }

    private static Dataset<Row> normalizeForMongo(Dataset<Row> df) {
        Dataset<Row> out = df;
        for (String c : df.columns()) {
            if (c.toLowerCase(Locale.ROOT).contains("revenue") ||
                    c.toLowerCase(Locale.ROOT).contains("spent") ||
                    c.toLowerCase(Locale.ROOT).contains("check") ||
                    c.toLowerCase(Locale.ROOT).contains("price")) {
                out = out.withColumn(c, col(c).cast("double"));
            }
        }
        return out;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static String getArg(String[] args, String name, String defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (name.equals(args[i])) {
                return args[i + 1];
            }
        }
        return defaultValue;
    }
}

