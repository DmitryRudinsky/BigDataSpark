package ru.mai.highload;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public class Job1_LoadStarSchema {
    public static void main(String[] args) {
        String pgUrl = getArg(args, "--pgUrl", envOrDefault("PG_URL", "jdbc:postgresql://postgres:5432/bigdata"));
        String pgUser = getArg(args, "--pgUser", envOrDefault("PG_USER", "admin"));
        String pgPassword = getArg(args, "--pgPassword", envOrDefault("PG_PASSWORD", "admin"));
        String pgTable = getArg(args, "--pgTable", envOrDefault("PG_TABLE", "public.mock_data"));

        SparkSession spark = SparkSession.builder()
                .appName("Job1_LoadStarSchema")
                .getOrCreate();

        Dataset<Row> mockData = spark.read()
                .format("jdbc")
                .option("url", pgUrl)
                .option("dbtable", pgTable)
                .option("user", pgUser)
                .option("password", pgPassword)
                .option("driver", "org.postgresql.Driver")
                .load();

        Dataset<Row> typed = mockData
                .withColumn("id_int", col("id").cast(DataTypes.IntegerType))
                .withColumn("customer_age_int", col("customer_age").cast(DataTypes.IntegerType))
                .withColumn("product_price_dec", col("product_price").cast(DataTypes.createDecimalType(12, 2)))
                .withColumn("product_quantity_int", col("product_quantity").cast(DataTypes.IntegerType))
                .withColumn("sale_customer_id_int", col("sale_customer_id").cast(DataTypes.IntegerType))
                .withColumn("sale_seller_id_int", col("sale_seller_id").cast(DataTypes.IntegerType))
                .withColumn("sale_product_id_int", col("sale_product_id").cast(DataTypes.IntegerType))
                .withColumn("sale_quantity_int", col("sale_quantity").cast(DataTypes.IntegerType))
                .withColumn("sale_total_price_dec", col("sale_total_price").cast(DataTypes.createDecimalType(12, 2)))
                .withColumn("sale_date_dt", to_date(col("sale_date"), "M/d/yyyy"))
                .withColumn("product_release_date_dt", to_date(col("product_release_date"), "M/d/yyyy"))
                .withColumn("product_expiry_date_dt", to_date(col("product_expiry_date"), "M/d/yyyy"))
                .withColumn("product_weight_dec", col("product_weight").cast(DataTypes.createDecimalType(12, 2)))
                .withColumn("product_rating_dec", col("product_rating").cast(DataTypes.createDecimalType(3, 1)))
                .withColumn("product_reviews_int", col("product_reviews").cast(DataTypes.IntegerType))
                .withColumn("store_key", sha2(concat_ws("||",
                        coalesce(col("store_name"), lit("")),
                        coalesce(col("store_location"), lit("")),
                        coalesce(col("store_city"), lit("")),
                        coalesce(col("store_state"), lit("")),
                        coalesce(col("store_country"), lit("")),
                        coalesce(col("store_phone"), lit("")),
                        coalesce(col("store_email"), lit(""))
                ), 256))
                .withColumn("supplier_key", sha2(concat_ws("||",
                        coalesce(col("supplier_name"), lit("")),
                        coalesce(col("supplier_email"), lit("")),
                        coalesce(col("supplier_phone"), lit("")),
                        coalesce(col("supplier_city"), lit("")),
                        coalesce(col("supplier_country"), lit(""))
                ), 256))
                .withColumn("sale_row_id", sha2(concat_ws("||",
                        coalesce(col("id"), lit("")),
                        coalesce(col("sale_date"), lit("")),
                        coalesce(col("sale_customer_id"), lit("")),
                        coalesce(col("sale_seller_id"), lit("")),
                        coalesce(col("sale_product_id"), lit("")),
                        coalesce(col("store_key"), lit("")),
                        coalesce(col("supplier_key"), lit("")),
                        coalesce(col("sale_quantity"), lit("")),
                        coalesce(col("sale_total_price"), lit(""))
                ), 256));

        ensureSchema(pgUrl, pgUser, pgPassword, "dw");

        Dataset<Row> dimCustomer = typed
                .select(
                        col("sale_customer_id_int").alias("customer_id"),
                        col("customer_first_name").alias("first_name"),
                        col("customer_last_name").alias("last_name"),
                        col("customer_age_int").alias("age"),
                        col("customer_email").alias("email"),
                        col("customer_country").alias("country"),
                        col("customer_postal_code").alias("postal_code"),
                        col("customer_pet_type").alias("pet_type"),
                        col("customer_pet_name").alias("pet_name"),
                        col("customer_pet_breed").alias("pet_breed")
                )
                .where(col("customer_id").isNotNull())
                .dropDuplicates("customer_id");

        Dataset<Row> dimSeller = typed
                .select(
                        col("sale_seller_id_int").alias("seller_id"),
                        col("seller_first_name").alias("first_name"),
                        col("seller_last_name").alias("last_name"),
                        col("seller_email").alias("email"),
                        col("seller_country").alias("country"),
                        col("seller_postal_code").alias("postal_code")
                )
                .where(col("seller_id").isNotNull())
                .dropDuplicates("seller_id");

        Dataset<Row> dimSupplier = typed
                .select(
                        col("supplier_key"),
                        col("supplier_name").alias("name"),
                        col("supplier_contact").alias("contact"),
                        col("supplier_email").alias("email"),
                        col("supplier_phone").alias("phone"),
                        col("supplier_address").alias("address"),
                        col("supplier_city").alias("city"),
                        col("supplier_country").alias("country")
                )
                .where(col("supplier_key").isNotNull())
                .dropDuplicates("supplier_key");

        Dataset<Row> dimStore = typed
                .select(
                        col("store_key"),
                        col("store_name").alias("name"),
                        col("store_location").alias("location"),
                        col("store_city").alias("city"),
                        col("store_state").alias("state"),
                        col("store_country").alias("country"),
                        col("store_phone").alias("phone"),
                        col("store_email").alias("email")
                )
                .where(col("store_key").isNotNull())
                .dropDuplicates("store_key");

        Dataset<Row> dimProduct = typed
                .select(
                        col("sale_product_id_int").alias("product_id"),
                        col("product_name").alias("name"),
                        col("product_category").alias("category"),
                        col("pet_category").alias("pet_category"),
                        col("product_price_dec").alias("price"),
                        col("product_quantity_int").alias("quantity_in_stock"),
                        col("product_weight_dec").alias("weight"),
                        col("product_color").alias("color"),
                        col("product_size").alias("size"),
                        col("product_brand").alias("brand"),
                        col("product_material").alias("material"),
                        col("product_description").alias("description"),
                        col("product_rating_dec").alias("rating"),
                        col("product_reviews_int").alias("reviews"),
                        col("product_release_date_dt").alias("release_date"),
                        col("product_expiry_date_dt").alias("expiry_date"),
                        col("supplier_key")
                )
                .where(col("product_id").isNotNull())
                .dropDuplicates("product_id");

        Dataset<Row> dimDate = typed
                .select(col("sale_date_dt").alias("full_date"))
                .where(col("full_date").isNotNull())
                .dropDuplicates("full_date")
                .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast(DataTypes.IntegerType))
                .withColumn("day", dayofmonth(col("full_date")))
                .withColumn("month", month(col("full_date")))
                .withColumn("year", year(col("full_date")))
                .withColumn("quarter", quarter(col("full_date")))
                .withColumn("day_of_week", dayofweek(col("full_date")))
                .select("date_key", "full_date", "day", "month", "year", "quarter", "day_of_week");

        Dataset<Row> factSales = typed
                .select(
                        col("sale_row_id"),
                        col("sale_customer_id_int").alias("customer_id"),
                        col("sale_seller_id_int").alias("seller_id"),
                        col("sale_product_id_int").alias("product_id"),
                        col("store_key"),
                        col("supplier_key"),
                        date_format(col("sale_date_dt"), "yyyyMMdd").cast(DataTypes.IntegerType).alias("date_key"),
                        col("sale_quantity_int").alias("sale_quantity"),
                        col("sale_total_price_dec").alias("total_price")
                )
                .where(col("sale_row_id").isNotNull());

        writeToPostgres(dimCustomer, pgUrl, pgUser, pgPassword, "dw.dim_customer");
        writeToPostgres(dimSeller, pgUrl, pgUser, pgPassword, "dw.dim_seller");
        writeToPostgres(dimSupplier, pgUrl, pgUser, pgPassword, "dw.dim_supplier");
        writeToPostgres(dimStore, pgUrl, pgUser, pgPassword, "dw.dim_store");
        writeToPostgres(dimProduct, pgUrl, pgUser, pgPassword, "dw.dim_product");
        writeToPostgres(dimDate, pgUrl, pgUser, pgPassword, "dw.dim_date");
        writeToPostgres(factSales, pgUrl, pgUser, pgPassword, "dw.fact_sales");

        System.out.println("DW tables written to schema dw");

        spark.stop();
    }

    private static void ensureSchema(String jdbcUrl, String user, String password, String schema) {
        String sql = "CREATE SCHEMA IF NOT EXISTS " + schema;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, password);
             Statement st = conn.createStatement()) {
            st.execute(sql);
        } catch (Exception e) {
            throw new RuntimeException("Failed to ensure schema exists: " + schema, e);
        }
    }

    private static void writeToPostgres(Dataset<Row> df, String jdbcUrl, String user, String password, String table) {
        df.write()
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", table)
                .option("user", user)
                .option("password", password)
                .option("driver", "org.postgresql.Driver")
                .option("truncate", "true")
                .mode("overwrite")
                .save();
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
