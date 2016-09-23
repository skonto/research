/**
  * Created by stavros on 23/9/2016.
  */
package sampling

import org.apache.spark.mllib.linalg.{Matrices, Matrix, Vectors}

object RunMarkov {

  def main(args: Array[String]): Unit = {

    List(2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2*1024, 4*1024, 16*1024).foreach { n =>

      var P = Matrices.dense(n, n, getPMatrix(n))

      var theta = Vectors.dense(Array.fill[Double](n)(1.0 / n))

      var initialTheta = Vectors.dense(
        Array(Math.pow(0.5, n - 1)) ++ (1 to (n - 1)).map { x => Math.pow(0.5, x) })
      var result = P.transpose.multiply(initialTheta)

      println(s"P num rows: ${P.numRows}")
      println(s"P num columns: ${P.numCols}")

      var calculatedPi = result
      var iterations = 1

      while (checkError(calculatedPi.values, 1.0 / n)) {
        calculatedPi = P.transpose.multiply(calculatedPi)
        iterations = iterations + 1
      }

      println(s"Iterations: $iterations")

      println(calculatedPi.values.mkString(","))
    }
  }

  def checkError(pi : Array[Double], error: Double): Boolean = {

    pi.map{x => Math.abs(x - error) > error*error}.reduce(_ || _) // error should be greater
  }

  def printMatrix(mat: Matrix): Unit = {
   mat.rowIter.foreach(row => println(row.toArray.mkString(",")))
  }

  def getPMatrix(n: Int): Array[Double] = {
    var initial: Array[Double] = Array(Math.pow(0.5, n-1)) ++ (1 to (n - 1)).map{x => Math.pow(0.5, x)}
    var result = initial
    (1 to n-1).foreach{  _ =>
      initial = {
        shiftRightOne(initial)
      }
      result = result ++ initial
    }
    println(s"size: ${result.length}")
    result
  }

  @inline
  def shiftRightOne(array: Array[Double]): Array[Double] = {
   Array(array.last) ++ array.slice(0, array.length -1)
  }
 }
