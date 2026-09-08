//import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.functions.{col, hash, pmod, lit, rand, floor}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataIngestionBenchmark")
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
      .config("spark.sql.catalog.local.s3.endpoint", "http://10.167.190.248:9000")
      .config("spark.sql.catalog.local.s3.path-style-access", "true")
      .config("spark.sql.catalog.local.client.region", "us-east-1")
      .config("spark.sql.catalog.local.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .config("spark.sql.catalog.local.s3.access-key-id", "admin")
      .config("spark.sql.catalog.local.s3.secret-access-key", "password123")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

//      .config("spark.hadoop.parquet.enable.summary-metadata", "false")
//      .config("spark.sql.parquet.mergeSchema", "false")
//      .config("spark.sql.parquet.filterPushdown", "true")
//      .config("spark.sql.hive.metastorePartitionPruning", "true")
//      .config("spark.executor.processTreeMetrics.enabled", "true")

//      .config("spark.hadoop.fs.s3a.committer.name", "directory")
//      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "file:///mnt/spark-shared/")

      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      .config("spark.hadoop.fs.s3a.committer.name", "magic")
      .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")


//      .config("spark.driver.memory", "16g")
//      .config("spark.executor.memory", "3g")
//      .master("local[12]")
//      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    // Odczyt surowych danych
//    val rawOrders = spark.read.parquet(args.apply(0))
//    val numRounds = 1
//    val executionTimes = new Array[Double](numRounds)

//    println(s"Rozpoczynanie benchmarku ($numRounds rund)...")
//    println(rawOrders.count())
//    for (i <- 0 until numRounds) {
//      val startParquet = System.nanoTime()
//      rawOrders
//        .withColumn("order_date", to_date(col("order_timestamp")))
//        .repartition(col("order_date"))
//        .write
//        .mode("append")
//        .partitionBy("order_date")
//        .parquet(args(1) + "_" + i.toString)
//      val endParquet = System.nanoTime()
//      val timeInSeconds = (endParquet - startParquet) / 1e9
//
//      executionTimes(i) = timeInSeconds
//      println(f"Runda ${i + 1}%2d: Czas zapisu Parquet = $timeInSeconds%.3f s")
//    }
////     WYŚCIG 1: Czysty Parquet z partycjonowaniem
//    val startParquet = System.nanoTime()
//    rawOrders
//      .withColumn("order_date", to_date(col("order_timestamp")))
//      .write
//      .option("compression", "uncompressed")
//      .mode("overwrite")
//      .partitionBy("order_date") // Ręczne partycjonowanie
//      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_order_items")
//      .parquet(args.apply(1))
//    val endParquet = System.nanoTime()

//     WYŚCIG 2: Iceberg z ukrytym partycjonowaniem
//    val startIceberg = System.nanoTime()
//    rawOrders.writeTo("local.ecommerce_db.order_items")
//      .replace()
//    val endIceberg = System.nanoTime()
//
//    println(s"Czas zapisu Parquet: ${(endParquet - startParquet) / 1e9} s")
//    println(s"Czas zapisu Iceberg: ${(endIceberg - startIceberg) / 1e9} s")
//    for (i <- 0 until numRounds) {
//      val startParquet = System.nanoTime()
    val rawProducts = spark.read.parquet("file:///mnt/spark-shared/products")
//    rawProducts.writeTo("local.ecommerce_db.products")
//      .append()
    val rawCustomers = spark.read.parquet("file:///mnt/spark-shared/customers")
//    rawCustomers.writeTo("local.ecommerce_db.customers")
//      .append()
    val rawOrders = spark.read.parquet("file:///mnt/spark-shared/orders_4g")
//    rawOrders.writeTo("local.ecommerce_db.orders")
//          .append()
    val rawOrderItems = spark.read.parquet("file:///mnt/spark-shared/order_items_4g")
//    rawOrderItems.writeTo("local.ecommerce_db.order_items")
//      .append()

//    rawProducts
//      .write
//      .mode("overwrite")
//      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_products")
//
//    rawCustomers
//      .write
//      .mode("overwrite")
//      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_customers")

    rawOrders
      .withColumn("order_date", to_date(col("order_timestamp")))
      .repartition(col("order_date"))
      .write
      .mode("overwrite")
      .partitionBy("order_date")
      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_orders")

//    rawOrderItems
//      .withColumn("order_id_bucket", pmod(hash(col("order_id")), lit(12)))
//      .withColumn("file_salt", floor(rand() * 12).cast("int"))
//      .repartition(col("order_id_bucket"), col("file_salt"))
//      .drop("file_salt")
//      .write
//      .mode("overwrite")
//      .partitionBy("order_id_bucket")
//      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_order_items")
//      val endParquet = System.nanoTime()
//      val timeInSeconds = (endParquet - startParquet) / 1e9
//
//      executionTimes(i) = timeInSeconds
//      println(f"Runda ${i + 1}%2d: Czas zapisu Iceberg = $timeInSeconds%.3f s")
//    }
//    val averageTime = executionTimes.sum / numRounds
//    executionTimes.foreach(println)
//    println("-" * 40)
//    println(f"Średni czas zapisu po $numRounds rundach: $averageTime%.3f s")
//    println("-" * 40)
    spark.stop()
  }
}

