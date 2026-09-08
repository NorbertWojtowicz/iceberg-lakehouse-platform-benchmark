import org.apache.spark.sql.SparkSession
import scala.concurrent.{Future, Await}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}


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
      .config("spark.executor.memory", "3854m")
      .config("spark.sql.iceberg.check-ordering", "false")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    println("Rozpoczynam test transakcyjności ACID (Równoległy UPDATE)...")

    // Transakcja A: Próbuje zmienić status na 'CANCELLED' dla zamówień z 2023 roku
    val transactionA = Future {
      println("Wątek A: Rozpoczynam UPDATE na CANCELLED...")
      spark.sql("""
        UPDATE local.ecommerce_db.orders
        SET status = 'CANCELLED'
        WHERE status = 'PENDING' AND order_timestamp >= '2023-01-01 00:00:00'
      """)
      "Wątek A: Sukces!"
    }

    // Transakcja B: Próbuje w tym samym czasie zmienić ten sam status na 'SHIPPED'
    val transactionB = Future {
      println("Wątek B: Rozpoczynam UPDATE na SHIPPED...")
      spark.sql("""
        UPDATE local.ecommerce_db.orders
        SET status = 'SHIPPED'
        WHERE status = 'PENDING' AND order_timestamp >= '2023-01-01 00:00:00'
      """)
      "Wątek B: Sukces!"
    }

    // Oczekiwanie na zakończenie obu wątków i przechwycenie wyników
    val results = Future.sequence(Seq(
      transactionA.transform(Success(_)),
      transactionB.transform(Success(_))
    ))

    val finalOutcome = Await.result(results, Duration.Inf)

    println("\n--- WYNIKI TRANSAKCJI RÓWNOLEGŁYCH ---")
    finalOutcome.foreach {
      case Success(msg) => println(s"Zakończono poprawnie: $msg")
      case Failure(ex) => println(s"Zablokowano transakcję (ACID): ${ex.getMessage}")
    }

    spark.stop()
  }
}

