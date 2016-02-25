package count_estimators

import com.facebook.stats.cardinality.HyperLogLogUtil._
import com.facebook.stats.cardinality.{BucketAndHash, Numbers, HyperLogLogUtil, HyperLogLog}
import com.google.common.base.Preconditions

sealed abstract class HLL {

  type Native

  def addObject(id: Long): Unit

  def counterEstimation(): Long

  def +(other: HLL): HLL

  private[count_estimators] val instance_ : Native
}


sealed abstract class HLLALGO

case object FBHLL extends HLLALGO

case object FBHLLEXT extends HLLALGO

object HLLFactory {

  def apply(numOfBuckets: Int, algoType:HLLALGO = FBHLL): HLL = {
       HLLFacebookFactory(numOfBuckets, algoType)
  }
}

object HLLFacebookFactory {

  def apply(numOfBuckets: Int, algoType:HLLALGO = FBHLL): HLL = {

    algoType match{
      case FBHLL =>  new HLLFacebook(new HyperLogLog(numOfBuckets))
      case FBHLLEXT => new HLLFacebookExtended(new ModifiedHyperLoglogFacebook(numOfBuckets))
    }
  }

  def apply(buckets: Array[Int], algoType:HLLALGO): HLL = {

    algoType match{
      case FBHLL =>    new HLLFacebook(new HyperLogLog(buckets))
      case FBHLLEXT =>    new HLLFacebookExtended(new ModifiedHyperLoglogFacebook(buckets))
    }
  }
}

class HLLFacebook(override val instance_ : HyperLogLog) extends HLL {

  type Native = HyperLogLog

  override def addObject(id: Long): Unit = instance_.add(id)

  override def counterEstimation(): Long = instance_.estimate()

  override def +(other: HLL): HLL = {
    HLLFacebookFactory(HyperLogLogUtil.mergeBuckets(instance_.buckets(),
      other.asInstanceOf[HLLFacebook].instance_.buckets()), FBHLL)
  }
}

class HLLFacebookExtended(override val instance_ : ModifiedHyperLoglogFacebook) extends HLL {

  type Native = ModifiedHyperLoglogFacebook

  override def addObject(id: Long): Unit = instance_.impl.add(id)

  override def counterEstimation(): Long = instance_.estimate()

  override def +(other: HLL): HLL = {
    HLLFacebookFactory(HyperLogLogUtil.mergeBuckets(instance_.impl.buckets(),
      other.asInstanceOf[HLLFacebookExtended].instance_.impl.buckets()), FBHLLEXT)
  }
}

class ModifiedHyperLoglogFacebook(val numOfbuckets: Int) {

  var impl: HyperLogLog = new HyperLogLog(numOfbuckets)

  def this(inpbuckets:Array[Int]){
    this(inpbuckets.length)
    impl = new HyperLogLog(inpbuckets)
  }


  def estimate(): Long = {

    val fs=impl.getClass.getDeclaredFields.filter{x=> x.getName == "currentSum" || x.getName == "nonZeroBuckets"}
    val currentSum= { val f=fs.find( x=> x.getName == "currentSum").get; f.setAccessible(true) ;
      val res=f.get(impl); f.setAccessible(false); res.asInstanceOf[Double] }

    val nonZeroBuckets={ val f=fs.find(x=>  x.getName == "nonZeroBuckets").get; f.setAccessible(true) ;
      val res=f.get(impl); f.setAccessible(false); res.asInstanceOf[Int] }

    val factor =Math.pow(2,32)
    val alpha: Double = HyperLogLogUtil.computeAlpha(impl.buckets.length)
    var result: Double = alpha * impl.buckets.length * impl.buckets.length / currentSum
    if (result <= 2.5 * impl.buckets.length) {
      val zeroBuckets: Int = impl.buckets.length - nonZeroBuckets
      if (zeroBuckets > 0) {
        result = impl.buckets.length * Math.log(impl.buckets.length * 1.0 / zeroBuckets)
      }
    }else if ( result <= factor / 30.0  ){
      result= result
    }
    else{ result = -factor* log2(1 - result/factor) }

    result.round
  }

  private def log2(x: Double) = scala.math.log(x)/scala.math.log(2)

}
