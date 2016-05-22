/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.thriftserver._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.SchemaRDD


// object SimpleApp {
//   def main(args: Array[String]) {
//     val logFile = "/Users/marvinvillareal/Developer/spark-1.6.1/README.md" // Should be some file on your system
//     val conf = new SparkConf().setAppName("Simple Application")
//     val sc = new SparkContext(conf)
//     val logData = sc.textFile(logFile, 2).cache()
//     val numAs = logData.filter(line => line.contains("a")).count()
//     val numBs = logData.filter(line => line.contains("b")).count()
//     println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
//   }
// }

object SimpleApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val hivectx = new HiveContext(sc)
    val filepath = "/Users/marvinvillareal/Developer/spark-1.6.1/examples/src/main/resources/people.json"
    val input = hivectx.jsonFile(filepath).registerTempTable("People")

    // hivectx.cacheTable("People")

    // val result = hivectx.sql("select * from People")
    // var paymentDataCache = result.cache
    // paymentDataCache.count
    // paymentDataCache.registerTempTable("paymentDataCache")


    HiveThriftServer2.startWithContext(hivectx)

    // val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //
    // sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
    // sqlContext.sql("LOAD DATA LOCAL INPATH '/Users/marvinvillareal/Developer/spark-1.6.1/examples/src/main/resources/kv1.txt' INTO TABLE src")
    //
    // // Queries are expressed in HiveQL
    // val result = sqlContext.sql("FROM src SELECT key, value")
    // result.cache()
    // result.count()
    // result.registerTempTable("mytable")
    //
    // // sqlContext.cacheTable("src")
    // HiveThriftServer2.startWithContext(sqlContext)

  }
}
