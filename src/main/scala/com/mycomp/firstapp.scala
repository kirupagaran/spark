/* SimpleApp.scala */

import org.apache.spark.SparkContext

import org.apache.spark.SparkContext._

import org.apache.spark.SparkConf

 

object SimpleApp {

  def main(args: Array[String]) {

    val logFile = "/user/spark/system.log" // Should be some file on your hdfs

    val conf = new SparkConf().setAppName("Simple Application")

    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()

    val numAs = logData.filter(line => line.contains("a"))

    numAs.saveAsTextFile(args(0));

  }

}
