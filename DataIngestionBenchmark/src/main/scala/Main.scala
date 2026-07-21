import org.apache.spark.sql.SparkSession

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
      .config("spark.hadoop.parquet.enable.summary-metadata", "false")
      .config("spark.sql.parquet.mergeSchema", "false")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.hive.metastorePartitionPruning", "true")

      .config("spark.hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "file:///mnt/spark-shared/")

      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
//      .config("spark.hadoop.fs.s3a.committer.name", "magic")
//      .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")


      .config("spark.executor.memory", "4g")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    // Odczyt surowych danych
    val rawOrders = spark.read.parquet("file:///mnt/spark-shared/order_items")
//    println(rawOrders.count())

//     WYŚCIG 1: Czysty Parquet z partycjonowaniem
    val startParquet = System.nanoTime()
    rawOrders
//      .withColumn("order_date", to_date(col("order_timestamp")))
      .write
//      .mode("overwrite")
//      .partitionBy("order_date") // Ręczne partycjonowanie
      .parquet("s3a://ecommerce-warehouse/warehouse/parquet_order_items")
    val endParquet = System.nanoTime()

//     WYŚCIG 2: Iceberg z ukrytym partycjonowaniem
//    val startIceberg = System.nanoTime()
//    rawOrders.writeTo("local.ecommerce_db.order_items")
//      .replace()
//    val endIceberg = System.nanoTime()
//
    println(s"Czas zapisu Parquet: ${(endParquet - startParquet) / 1e9} s")
//    println(s"Czas zapisu Iceberg: ${(endIceberg - startIceberg) / 1e9} s")
  }
}

