import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ECommerceDataGenerator")
      .config("spark.hadoop.fs.s3a.endpoint", "http://10.167.190.248:9000") // Zmień na swój host/IP
      .config("spark.hadoop.fs.s3a.access.key", "admin")
      .config("spark.hadoop.fs.s3a.secret.key", "password123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.sql.shuffle.partitions", "16")
      .config("spark.executor.memory", "4g")
      .master("spark://10.167.190.31:7090")
      .getOrCreate()

    val numCustomers = 1000000
    val numProducts = 500000
    val numOrders = 50000000

    val landingPath = "s3a://landing/"

    println("Generating data to the Landing Zone...")

//     1. Clients
    val customersDF = spark.range(1, numCustomers + 1)
      .withColumn("customer_id", col("id"))
      .withColumn("email", concat(lit("user_"), col("id"), lit("@ecommerce.com")))
      .withColumn("city", expr("element_at(array('Warszawa', 'Kraków', 'Wrocław', 'Gdańsk', 'Poznań', 'Łódź'), cast(rand() * 6 + 1 as int))"))
      .withColumn("country", lit("Polska"))
      .withColumn("registration_date", expr("date_add(date('2019-01-01'), cast(rand() * 1800 as int))"))
      .drop("id")

    customersDF.write.mode("overwrite").parquet(landingPath + "customers")
    println(s"Saved $numCustomers clients.")

//     2. Products
    println("Generating products...")
    val productsDF = spark.range(1, numProducts + 1)
      .withColumn("product_id", col("id"))
      .withColumn("name", concat(lit("Product_"), col("id")))
      .withColumn("category", expr("element_at(array('Elektronika', 'Dom', 'Ogród', 'Sport', 'Odzież'), cast(rand() * 5 + 1 as int))"))
      .withColumn("base_price", round(expr("rand() * 990 + 10"), 2))
      .withColumn("manufacturer", expr("element_at(array('Samsung', 'Sony', 'Apple', 'Philips', 'LG'), cast(rand() * 5 + 1 as int))"))
      .drop("id")

    productsDF.write.mode("overwrite").parquet(landingPath + "products")
    println(s"Saved $numProducts products.")

//    3. Orders
    val ordersDF = spark.range(1, numOrders + 1)
      .withColumn("order_id", col("id"))
      .withColumn("customer_id", expr(s"cast(rand() * $numCustomers + 1 as bigint)"))
      .withColumn("order_timestamp", expr("timestamp('2022-01-01 00:00:00') + interval 1 second * cast(rand() * 63072000 as int)"))
      .withColumn("status", expr("element_at(array('COMPLETED', 'PENDING', 'SHIPPED'), cast(rand() * 3 + 1 as int))"))
      .withColumn("total_amount", round(expr("rand() * 1500 + 50"), 2))
      .drop("id")

    ordersDF.cache()

    ordersDF.write.mode("overwrite").parquet(landingPath + "orders")
    println(s"Saved $numOrders orders.")

//     4. Order Items
    val orderItemsDF = ordersDF.select("order_id")
      .withColumn("num_items", expr("cast(rand() * 4 + 1 as int)"))
      .withColumn("items_array", expr("sequence(1, num_items)"))
      .withColumn("item_index", explode(col("items_array")))
      .withColumn("item_id", monotonically_increasing_id())
      .withColumn("product_id", expr(s"cast(rand() * $numProducts + 1 as bigint)"))
      .withColumn("quantity", expr("cast(rand() * 3 + 1 as int)"))
      .withColumn("unit_price", round(expr("rand() * 500 + 10"), 2))
      .select("item_id", "order_id", "product_id", "quantity", "unit_price")

    orderItemsDF.write.mode("overwrite").parquet(landingPath + "order_items")
    println(s"Saved $numOrders order items.")


    println("Data generation successfully finished.")
    spark.stop()
  }
}

