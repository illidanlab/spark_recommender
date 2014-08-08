package com.samsung.vddil.recsys.evaluation

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.testing.HitSet
import com.samsung.vddil.recsys.utils.Logger

/**
 * Defines the type of metric to be used in evaluation
 */
sealed trait RecJobMetric{
    /** Name of the metric */
    def metricName: String
    
    /** Parameters of the metric */
    def metricParams: HashMap[String, String]
}

/** Generic metric type of squared error */
trait RecJobMetricSE extends RecJobMetric {
	def run(labelNPred: RDD[(Double, Double)]): Double
}

/** Metric type of hit rate */
case class RecJobMetricHR(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetric {
	//will calculate average hit rate across passed user hits for all items
    // and new items 
    def run(hitSets:RDD[HitSet]):(Double, Double) = {
    	Logger.info("Count of hit sets: " + hitSets.count)
      //get hit-rate on combined train and test 
      val combHR = hitSets.map {hitSet =>
            val allHRInters = (hitSet.topNPredAllItem.toSet & 
                                   hitSet.topNTestAllItems.toSet).size.toDouble
            (allHRInters/hitSet.N, 1)
      }.reduce((a,b) => (a._1+b._1, a._2+b._2))
      val numUsers = combHR._2
      val avgCombHR = combHR._1/numUsers
  
      //get hit-rate on test items if there was new items in
      //test, also while calculating recall divide by no. of new items in test
      //if less than N
      val testHR = hitSets.map{hitSet =>
        val testInters = (hitSet.topNPredNewItems.toSet & 
                                   hitSet.topNTestNewItems.toSet).size.toDouble
        val currHR = hitSet.topNTestNewItems.length match {
          case 0 => (0.0, 0.0)
          case a:Int => (testInters/hitSet.topNTestNewItems.length, 1.0)
        }
        currHR
      }.reduce((a,b) => (a._1+b._1, a._2+b._2))

      val numTestUsers = testHR._2
      val avgTestHR = testHR._1/numTestUsers
      
      val avgHitRate = (avgCombHR, avgTestHR)
      avgHitRate
    }
}

/** metric type of recall for cold items recommendation**/
case class RecJobMetricColdRecall(metricName:String, metricParams: HashMap[String,
  String]) extends RecJobMetric {
  /**
   * @param topNPredColdItems RDD of topN predicted set and size of 
   * intersection with cold items
   */
  def run(topNPredColdItems:RDD[(Int, (List[String], Int))]):Double = {
    val (recallSum, count) = topNPredColdItems.map{x =>
      //ideally it should be N, but some times the number of cold items can be
      //less than N for a user
      val topNSize:Int = x._2._1.length
      //compute recall by dividing intersection with actual sold items by N or
      //topNSize
      val recall:Double = x._2._2.toDouble/topNSize
      (recall, 1)
    }.reduce{(a,b) =>
      (a._1+b._1, a._2+b._2)    
    }
    (recallSum*1.0)/count
  }
}



/** Metric type of mean squared error */
case class RecJobMetricMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
	def run(labelNPred: RDD[(Double, Double)]): Double = {
		//NOTE: can be further extended to use metricName and metricParams like test and model
		ContinuousPrediction.computeMSE(labelNPred)
	}
}

/** Metric type of root mean squared error */
case class RecJobMetricRMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
    def run(labelNPred: RDD[(Double, Double)]): Double = {
        //NOTE: can be further extended to use metricName and metricParams like test and model
        ContinuousPrediction.computeRMSE(labelNPred)
    }
}
