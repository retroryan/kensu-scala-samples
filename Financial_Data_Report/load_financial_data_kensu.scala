
import org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Load Financial Data")
  .config("spark.driver.extraClassPath", "kensu-dam-spark-collector-0.23.5-SNAPSHOT_spark-3.2.1.jar")
  .getOrCreate()

implicit val ch = new io.kensu.dodd.sdk.ConnectHelper("conf.ini")
io.kensu.dam.lineage.spark.lineage.Implicits.SparkSessionDAMWrapper(spark).track(ch.properties.get("dam.ingestion.url").map(_.toString), None)(ch.properties.toList:_*)


def loadStock(year:String, month:String, stock:String):org.apache.spark.sql.DataFrame = {
  spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(s"datasources/$year/$month/$stock.csv")
    .withColumnRenamed("Adj Close", "AdjClose")
}

//    .withColumn("Intraday_Delta", expr("AdjClose - Open"))


val year = "2021"
val month = "nov"

val apple_stock_df = loadStock(year,month, "Apple")
val apptech_stock_df = loadStock(year,month, "AppTech")
val buzzfeed_stock_df = loadStock(year,month, "Buzzfeed")
val eur_usd_df = loadStock(year,month, "Apple")
val imetal_df = loadStock(year,month, "AppTech")
val microsoft_stock_df = loadStock(year,month, "Buzzfeed")

microsoft_stock_df.show()

val dfSeq = Seq(apple_stock_df, apptech_stock_df, buzzfeed_stock_df,eur_usd_df,imetal_df,microsoft_stock_df)
val monthly_assets_df = dfSeq.reduce(_ union _)

monthly_assets_df.show()

monthly_assets_df.write.mode("overwrite").save(s"datasources/$year/$month/monthly")

monthly_assets_df.registerTempTable("tmp_monthly_assets")


//sql("SELECT * FROM tmp_monthly_assets where symbol = 'BZFD'").write.mode("overwrite").csv("just_bzfd")
