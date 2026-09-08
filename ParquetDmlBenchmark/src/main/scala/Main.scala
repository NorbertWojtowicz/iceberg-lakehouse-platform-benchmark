import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataIngestionBenchmark")
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
      .config("spark.executor.memory", "3584m")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()


    println("Rozpoczynam testy DML dla surowego formatu Parquet...")

    val parquetPath = args.apply(0)

    val numRounds = 12
    val executionTimesUpdate = new Array[Double](numRounds)
    // --- TEST 1: UPDATE (Read-Modify-Write) ---
    for (i <- 1 until numRounds) {
      val formattedMonth = f"$i%02d"
      val nexti = i + 1
      val year = args.apply(0)
      val formattedNextMonth = f"$nexti%02d"
      val startDate = args.apply(1) + "-" + formattedMonth + "-01 00:00:00"
      val endDate = args.apply(1) + "-" + formattedNextMonth + "-01 00:00:00"
      val updateStartTime = System.nanoTime()
      // 1. Wczytanie całej tabeli
      val df = spark.read.parquet(parquetPath)

      // 2. Aplikacja logiki warunkowej (UPDATE)
      val updatedDf = df.withColumn("status",
        when(
          col("status") === "PENDING" &&
            col("order_timestamp") >= startDate &&
            col("order_timestamp") < endDate,
          lit("SHIPPED")
        ).otherwise(col("status"))
      )
      // 3. Zapisanie danych
      updatedDf.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(parquetPath)

      val updateEndTime = System.nanoTime()
      val updateDuration = (updateEndTime - updateStartTime) / 1e9d
      println(f"Czas operacji UPDATE (Parquet): $updateDuration%.3f sekund")
      executionTimesUpdate(i) = updateDuration
    }
    val averageTime = executionTimesUpdate.sum / numRounds
    executionTimesUpdate.foreach(println)
    println("-" * 40)
    println(f"Średni czas operacji UPDATE po $numRounds rundach: $averageTime%.3f s")
    println("-" * 40)


    val executionTimesDelete = new Array[Double](numRounds)

    for (i <- 1 until numRounds) {
      val formattedMonth = f"$i%02d"
      val nexti = i + 1
      val year = args.apply(0)
      val formattedNextMonth = f"$nexti%02d"
      val startDate = args.apply(1) + "-" + formattedMonth + "-01 00:00:00"
      val endDate = args.apply(1) + "-" + formattedNextMonth + "-01 00:00:00"
      // --- TEST 2: DELETE (Filtrowanie i nadpisanie) ---
      val deleteStartTime = System.nanoTime()

      val dfForDelete = spark.read.parquet(parquetPath)

      val deletedDf = dfForDelete.filter(
        not(
          col("status") === "CANCELLED" &&
            col("order_timestamp") >= startDate &&
            col("order_timestamp") < endDate
        )
      )

      // Nadpisanie przefiltrowanych danych
      deletedDf.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(parquetPath)

      val deleteEndTime = System.nanoTime()
      val deleteDuration = (deleteEndTime - deleteStartTime) / 1e9d
      println(f"Czas operacji DELETE (Parquet): $deleteDuration%.3f sekund")
      executionTimesDelete(i) = deleteDuration
    }
    val averageTimeDelete = executionTimesDelete.sum / numRounds
    executionTimesDelete.foreach(println)
    println("-" * 40)
    println(f"Średni czas operacji DELETE po $numRounds rundach: $averageTimeDelete%.3f s")
    println("-" * 40)


    spark.stop()
  }
}

