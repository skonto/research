package count_estimators

import org.junit.runner.RunWith
import org.scalatest.{ShouldMatchers, FunSuite}
import org.scalatest.junit.JUnitRunner

import scala.collection.parallel.ForkJoinTaskSupport
import scala.util.Random

case class TaxData(f_name:String, f_value:String)

@RunWith(classOf[JUnitRunner])
class HLLTest extends FunSuite with ShouldMatchers{


  test("HLL basic") {

    println(s"inserts,registers, nodes, One counter, Sum, Merge")

    List(100,1000).foreach{ nodes=>
      //Math.pow(2, 8).toInt, Math.pow(2, 10).toInt,
      List( Math.pow(2, 16).toInt).
        foreach { regs =>

        (0 to 1000000 by 100000).tail.foreach { counter =>

          val numOfNodes = nodes
          val numOfInserts = counter

          val numOfRegirsters = regs

          val rand1 = new Random()
          val rand2 = new Random()

          //one node sum counters reference sum
          val hll = HLLFactory(numOfRegirsters)
          val insertsPerNode = numOfInserts / numOfNodes
          (1 to numOfInserts).foreach {
            x => //if (x % insertsPerNode == 0) println(x / insertsPerNode + "M");
              hll.addObject(rand1.nextLong())
          }

          // sum from different nodes
          val res1 = (1 to numOfNodes).map { x => val h = HLLFactory(numOfRegirsters)
            ((x - 1) * insertsPerNode + 1 to x * insertsPerNode).foreach {
              y => h.addObject(rand2.nextLong())
            }
            h
          }

          //merge buckets
          val res3 = res1.reduce { (x1: HLL, x2: HLL) =>
            x1 + x2
          }.counterEstimation()

          println(s"$counter $regs $numOfNodes ${hll.counterEstimation()} ${res1.map { x => x.counterEstimation() }.sum} " +
            s"${res3} ${relative(hll.counterEstimation(), numOfInserts)} ${relative(res1.map { x => x.counterEstimation() }.sum,numOfInserts)}" +
            s" ${relative(res3, numOfInserts)}")

        }
      }
    }
  }



  test("HLL extended") {

    println(s"inserts,registers, nodes, One counter, Sum, Merge")

    List(100,1000).foreach{ nodes=>
//Math.pow(2, 8).toInt, Math.pow(2, 10).toInt,
    List(Math.pow(2, 12).toInt, Math.pow(2, 14).toInt).
      foreach { regs =>

      (0 to 1000000 by 100000).tail.foreach { counter =>

          val numOfNodes = nodes
          val numOfInserts = counter

          val numOfRegirsters = regs

          val rand1 = new Random()
          val rand2 = new Random()

          //one node sum counters reference sum
          val hll = HLLFactory(numOfRegirsters, FBHLLEXT)
          val insertsPerNode = numOfInserts / numOfNodes
          (1 to numOfInserts).foreach {
            x => //if (x % insertsPerNode == 0) println(x / insertsPerNode + "M");
              hll.addObject(rand1.nextLong())
          }

          // sum from different nodes
          val res1 = (1 to numOfNodes).map { x => val h = HLLFactory(numOfRegirsters, FBHLLEXT)
            ((x - 1) * insertsPerNode + 1 to x * insertsPerNode).foreach {
              y => h.addObject(rand2.nextLong())
            }
            h
          }

          //merge buckets
          val res3 = res1.reduce { (x1: HLL, x2: HLL) =>
            x1 + x2
          }.counterEstimation()

          println(s"$counter $regs $numOfNodes ${hll.counterEstimation()} ${res1.map { x => x.counterEstimation() }.sum} " +
            s"${res3} ${relative(hll.counterEstimation(), numOfInserts)} ${relative(res1.map { x => x.counterEstimation() }.sum,numOfInserts)}" +
            s" ${relative(res3, numOfInserts)}")

        }
      }
    }
  }


  def relative(data:Long,ref:Double)=Math.abs((data- ref)/ref)
}
