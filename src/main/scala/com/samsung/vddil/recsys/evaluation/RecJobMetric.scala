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
        //TODO:  case when there was no new item in test 
        val hitScores = hitSets.map {hitSet =>
            val allHRInters = (hitSet.topNPredAllItem.toSet & 
                                   hitSet.topNTestAllItems.toSet).size.toDouble
            val newHRInters = (hitSet.topNPredNewItems.toSet & 
                                   hitSet.topNTestNewItems.toSet).size.toDouble
            (allHRInters/hitSet.N, newHRInters/hitSet.N, 1)
        }.reduce((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3))
        val numUsers = hitScores._3
        val avgHitRate = (hitScores._1/numUsers, hitScores._2/numUsers)
        avgHitRate
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