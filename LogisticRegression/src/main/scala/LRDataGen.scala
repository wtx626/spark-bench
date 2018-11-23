import org.apache.spark.SparkContext
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
      s"${y}${x.foreach(p=>s",${p}").toString}"
    }
    data
  }
}
