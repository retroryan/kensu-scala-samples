import org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("Load Financial Data")
  .config("spark.driver.extraClassPath", "kensu-dam-spark-collector-0.23.5-SNAPSHOT_spark-3.2.1.jar")
  .getOrCreate()

implicit val ch = new io.kensu.dodd.sdk.ConnectHelper("conf.ini")
io.kensu.dam.lineage.spark.lineage.Implicits.SparkSessionDAMWrapper(spark).track(ch.properties.get("dam.ingestion.url").map(_.toString), None)(ch.properties.toList:_*)

val year = "2021"
val month = "nov"

//Core Script: Extract data from the monthly_assets data source and create 2 reports with a new column
val all_assets = spark.read.option("inferSchema","true").option("header","true").parquet(s"datasources/$year/$month/monthly")

val apptech = all_assets.filter("Symbol == 'APCX'")
val Buzzfeed = all_assets.filter("Symbol == 'ENFA'")


val buzz_report = Buzzfeed.withColumn("Intraday_Delta", expr("AdjClose - Open"))
val apptech_report = apptech.withColumn("Intraday_Delta", expr("AdjClose - Open"))


val final_report_buzzfeed = buzz_report.drop("High","Low","Close","Volume")
val final_report_apptech = apptech_report.drop("High","Low","Close","Volume")

final_report_buzzfeed.write.mode("overwrite").save(s"datasources/$year/$month/report_buzzfeed")
final_report_apptech.write.mode("overwrite").save(s"datasources/$year/$month/report_AppTech")
