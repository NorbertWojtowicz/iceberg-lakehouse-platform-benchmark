import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IcebergModelInitialization")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type", "hive")
      .config("spark.sql.catalogImplementation", "hive")
      .config("spark.sql.hive.thriftServer.singleSession", "false")
      .config("spark.hive.metastore.uris", "thrift://10.167.190.85:9083")
      .config("spark.hive.metastore.schema.verification", "false")
      .config("spark.sql.catalog.local.warehouse", "s3a://ecommerce-warehouse/")
      .config("spark.hadoop.fs.s3a.endpoint", "http://10.167.190.248:9000")
      .config("spark.hadoop.fs.s3a.access.key", "admin")
      .config("spark.hadoop.fs.s3a.secret.key", "password123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    // Default catalog
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.ecommerce_db")
    spark.sql("USE local.ecommerce_db")


    // Clients Table (Without partitioning - small volume)
    spark.sql("""
      CREATE TABLE IF NOT EXISTS customers (
        customer_id BIGINT,
        email STRING,
        city STRING,
        country STRING,
        registration_date DATE
      ) USING iceberg
      TBLPROPERTIES ('write.format.default'='parquet')
    """)

    // Products Table (Without partitioning)
    spark.sql("""
      CREATE TABLE IF NOT EXISTS products (
        product_id BIGINT,
        name STRING,
        category STRING,
        base_price DECIMAL(18, 2),
        manufacturer STRING
      ) USING iceberg
    """)


    // Orders Table (Hidden Partitioning by days)
    spark.sql("""
      CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT,
        customer_id BIGINT,
        order_timestamp TIMESTAMP,
        status STRING,
        total_amount DECIMAL(18, 2)
      )
      USING iceberg
      PARTITIONED BY (days(order_timestamp))
      TBLPROPERTIES (
        'format-version' = '2',
        'write.upsert.enabled' = 'true'
      )
    """)

    // Order Items Table - Partitioning by Bucket
    spark.sql("""
      CREATE TABLE IF NOT EXISTS order_items (
        item_id BIGINT,
        order_id BIGINT,
        product_id BIGINT,
        quantity INT,
        unit_price DECIMAL(18, 2)
      )
      USING iceberg
      PARTITIONED BY (bucket(16, order_id))
      TBLPROPERTIES ('format-version' = '2')
    """)

    println("Data Model successfully initialized in the Iceberg format!")
    spark.stop()
  }
}
