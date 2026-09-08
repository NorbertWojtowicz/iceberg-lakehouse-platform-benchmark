import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date

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

      //      .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
      //      .config("spark.sql.parquet.output.committer.class", "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
      //      .config("spark.hadoop.fs.s3a.committer.name", "magic")
      //      .config("spark.hadoop.fs.s3a.committer.magic.enabled", "true")


      //      .config("spark.driver.memory", "16g")
      .config("spark.executor.memory", "3854m")
      //      .master("local[12]")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    println("Rozpoczynam benchmark analityczny OLAP dla Apache Iceberg...")

    val queries = Map(
      "Q3 (Pełny skan i złączenia wielotabelowe)" ->
        """SELECT c.city, p.category, SUM(i.quantity * i.unit_price) AS total_revenue
           FROM local.ecommerce_db.order_items i
           JOIN local.ecommerce_db.orders o ON i.order_id = o.order_id
           JOIN local.ecommerce_db.products p ON i.product_id = p.product_id
           JOIN local.ecommerce_db.customers c ON o.customer_id = c.customer_id
           GROUP BY c.city, p.category
           ORDER BY total_revenue DESC LIMIT 10"""
    )

    for ((name, sql) <- queries) {
      println(s"Uruchamiam: $name")

      // Cold Start
//      spark.sql(sql).collect()

      var totalTime = 0.0
      val runs = 10
      val executionTimesDelete = new Array[Double](runs+2)
//      for (i <- 1 to runs) {
        val t0 = System.nanoTime()
        spark.sql(sql).collect()
        val t1 = System.nanoTime()
//        executionTimesDelete(i) = (t1 - t0) / 1e9d
        totalTime += (t1 - t0) / 1e9d
//      }
//      executionTimesDelete.foreach(println)
      println(f"---> Średni czas Warm Start ($name): ${totalTime / runs}%.3f sekund\n")
    }

    spark.stop()
  }
}

