import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created with IntelliJ IDEA. 
  * User: wutianxiong 
  * Date: 2018/11/23 
  * Time: 16:35
  */
object LRDataGen {

  def generateLogisticRDD(
                           sc: SparkContext,
                           nexamples: Int,
                           nfeatures: Int,
                           eps: Double,
                           nparts: Int = 2,
                           probOne: Double = 0.5): RDD[String] = {
    val data = sc.parallelize(0 until nexamples, nparts).map { idx =>
      val rnd = new Random(42 + idx)

      val y = if (idx % 2 == 0) 0.0 else 1.0
      val x = Array.fill[Double](nfeatures) {
        rnd.nextGaussian() + (y * eps)
      }
      var tmp = ""
      for (point <- x) {
        tmp += s",$point"
      }
      s"${y}${tmp}"
    }
    data
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.out.println("usage: <output> <nExamples> <nFeatures> <eps> <probone> [numPar]")
      System.exit(0)
    }
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val output = args(0)
    val nExamples = args(1).toInt
    val nFeatures = args(2).toInt
    val eps = args(3).toDouble
    val probOne = args(4).toDouble
    val numPar = if (args.length > 5) args(5).toInt
    else if (System.getProperty("spark.default.parallelism") != null) System.getProperty("spark.default.parallelism").toInt
    else 2

    val conf = new SparkConf().setAppName("Logisitc Regression data generation")
    val sc = new SparkContext(conf)
    val data = generateLogisticRDD(sc, nExamples, nFeatures, eps, numPar, probOne)
    val parsedData = data
    parsedData.saveAsTextFile(output)
    sc.stop()
  }
}
