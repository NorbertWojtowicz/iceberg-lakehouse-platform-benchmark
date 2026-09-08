import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.to_date
//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
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

    val numRounds = 12
    val executionTimes = new Array[Double](numRounds)

    println("Rozpoczynam testy DML dla Apache Iceberg...")

    // --- TEST 1: UPDATE (Aktualizacja wierszy) ---
    // Symulacja: Zmiana statusu z PENDING na SHIPPED dla zamówień z wybranego miesiąca


//    spark.sql("""
//      UPDATE local.ecommerce_db.fct_orders
//      SET status = 'SHIPPED'
//      WHERE status = 'PENDING'
//        AND order_timestamp >= '2023-05-01 00:00:00'
//        AND order_timestamp < '2023-06-01 00:00:00'
//    """)
    for (i <- 1 until numRounds) {
      val formattedMonth = f"$i%02d"
      val nexti = i + 1
      val year = args.apply(0)
      val formattedNextMonth = f"$nexti%02d"
      val updateStartTime = System.nanoTime()
      spark.sql(s"""
        UPDATE local.ecommerce_db.orders
        SET status = 'SHIPPED'
        WHERE status = 'PENDING'
          AND order_timestamp >= '$year-$formattedMonth-01 00:00:00'
          AND order_timestamp < '$year-$formattedNextMonth-01 00:00:00'
      """)
      val updateEndTime = System.nanoTime()
      val updateDuration = (updateEndTime - updateStartTime) / 1e9d
      executionTimes(i) = updateDuration
      println(f"Czas operacji UPDATE (Iceberg): $updateDuration%.3f sekund")
    }
    val averageTime = executionTimes.sum / numRounds
    executionTimes.foreach(println)
    println("-" * 40)
    println(f"Średni czas operacji UPDATE po $numRounds rundach: $averageTime%.3f s")
    println("-" * 40)


    val executionTimesDelete = new Array[Double](numRounds)

    for (i <- 1 until numRounds) {
      val formattedMonth = f"$i%02d"
      val nexti = i + 1
      val year = args.apply(0)
      val formattedNextMonth = f"$nexti%02d"
      val deleteStartTime = System.nanoTime()

      spark.sql(s"""
        DELETE FROM local.ecommerce_db.orders
        WHERE status = 'CANCELLED'
          AND order_timestamp >= '$year-$formattedMonth-01 00:00:00'
          AND order_timestamp < '$year-$formattedNextMonth-01 00:00:00'
      """)
      val deleteEndTime = System.nanoTime()
      val deleteDuration = (deleteEndTime - deleteStartTime) / 1e9d
      executionTimesDelete(i) = deleteDuration
      println(f"Czas operacji DELETE (Iceberg): $deleteDuration%.3f sekund")
    }

//    // --- TEST 2: DELETE (Usuwanie wierszy) ---
//    // Symulacja: Usunięcie anulowanych zamówień (RODO / prawo do usunięcia)
//    val deleteStartTime = System.nanoTime()
//
//    spark.sql("""
//      DELETE FROM local.ecommerce_db.fct_orders
//      WHERE status = 'CANCELLED'
//        AND order_timestamp >= '2022-01-01 00:00:00'
//        AND order_timestamp < '2022-02-01 00:00:00'
//    """)
//



    val averageTimeDelete = executionTimesDelete.sum / numRounds
    executionTimesDelete.foreach(println)
    println("-" * 40)
    println(f"Średni czas operacji DELETE po $numRounds rundach: $averageTimeDelete%.3f s")
    println("-" * 40)

    spark.stop()
  }
}

