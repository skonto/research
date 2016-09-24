/**
  * Created by stavros on 23/9/2016.
  */
package sampling

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.Vector

object RunDistributedMarkov {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf()

    sparkConfig.setMaster("local[*]")

    sparkConfig.setAppName("random sampling")

    val sc = new SparkContext(sparkConfig)

    sc.setLogLevel("Error")
    List( 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2*1024, 4*1024, 16*1024).foreach { n =>

      var P = new RowMatrix(getPMatrix(n, sc), n, n)

      var initialTheta = Matrices.dense(n, 1,
        Array(Math.pow(0.5, n - 1)) ++ (1 to (n - 1)).map { x => Math.pow(0.5, x) })
      var result = P.multiply(initialTheta)

      println(s"P num rows: ${P.numRows}")
      println(s"P num columns: ${P.numCols}")

      var calculatedPi = result.rows.collect().map(_.toArray).reduce(_ ++ _)
      var iterations = 1

      println(calculatedPi.mkString(","))
      while (checkError(calculatedPi, 1.0 / n)) {

        calculatedPi = P.multiply(Matrices.dense(n, 1, calculatedPi))
          .rows.collect().map(_.toArray).reduce(_ ++ _)
        iterations = iterations + 1
      }

      println(s"Iterations: $iterations")

      println(calculatedPi.mkString(","))
    }
  }

  def checkError(pi : Array[Double], error: Double): Boolean = {

    pi.map{x => Math.abs(x - error) > error*error}.reduce(_ || _) // error should be greater
  }


  def getPMatrix(n: Int, sc: SparkContext): RDD[Vector] = {
    var initial: Array[Double] = Array(Math.pow(0.5, n-1)) ++ (n-1 to 1 by -1).map{x => Math.pow(0.5, x)}
    println(s"Initial:${initial.mkString(",")}")
    var result = Array(Vectors.dense(initial))
    (1 to n-1).foreach{  _ =>
      initial = {
        shiftRightOne(initial)
      }
      result = result ++ Array(Vectors.dense(initial))
    }
     sc.parallelize(result)
  }

  @inline
  def shiftRightOne(array: Array[Double]): Array[Double] = {
   Array(array.last) ++ array.slice(0, array.length -1)
  }
 }
