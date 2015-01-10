package com.samsung.vddil.recsys.evaluation

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.testing.HitSet
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.Pipeline
import org.apache.hadoop.fs.Path

/**
 * Defines the type of metric to be used in evaluation
 */
sealed trait RecJobMetric{
    /** Name of the metric */
    def metricName: String
    
    /** Parameters of the metric */
    def metricParams: HashMap[String, String]
    
    /** An identity prefix*/
    def IdenPrefix:String
    
    /**
     * Generate an unique identity for metric. 
     */
    def resourceIdentity():String = {
        IdenPrefix + "_" + HashString.generateHash(metricParams.toString)
    }
}

object RecJobMetric{
    type MetricResult = Map[String, Double]
    
    def emptyResult(): MetricResult = {
        Map[String, Double]()
    }
    
    /**
     * Serialize the metric to a specific file. 
     */
    private[recsys] def saveMetricResult(
            metricFile:String, 
            computedMetricResult:RecJobMetric.MetricResult) = {
        
        Logger.info("Save metric file: "+ metricFile)
		val out = Pipeline.instance.get.fs.create(new Path(metricFile))
		val ser2 = Pipeline.instance.get.kryo.serializeStream(out).writeObject(computedMetricResult)
        ser2.close()
        out.close()
    }
    
    /**
     * Deserialize the metric from a specified file. 
     */
    private[recsys] def loadMetricResult(
            	metricFile:String
            ):RecJobMetric.MetricResult = {
        
        Logger.info("Load metric file: "+ metricFile)
	    val in = Pipeline.instance.get.fs.open(new Path(metricFile))
        val metricResult = Pipeline.instance.get.kryo.deserializeStream(in).
            		readObject[RecJobMetric.MetricResult]()
        in.close()
        
        metricResult
    }
}


/** Metric type of hit rate */
case class RecJobMetricHR(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetric {
    
    
    val IdenPrefix = "MetricHitRate"
    
	//will calculate average hit rate across passed user hits for all items
    // and new items 
    def run(hitSets:RDD[HitSet]):RecJobMetric.MetricResult = {
        
      Logger.info("Count of hit sets: " + hitSets.count)
      //get hit-rate on combined train and test 
      val combHR = hitSets.map {hitSet =>
            val allHRInters = (hitSet.topNPredAllItem.toSet & 
                                   hitSet.topNTestAllItems.toSet).size.toDouble
            //Debug info.                        
            //val size_pred = hitSet.topNPredAllItem.toSet.size
            //val size_all  = hitSet.topNTestAllItems.toSet.size
            //val size_inter = (hitSet.topNPredAllItem.toSet & 
            //                       hitSet.topNTestAllItems.toSet).size
            (1, 
             allHRInters/hitSet.topNTestAllItems.length, 
             allHRInters,  
             allHRInters/hitSet.N)
      }.reduce((a,b) => (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4))
      val numUsers  = combHR._1
      val recall    = combHR._2/numUsers // recall=> avg(#userHit/#userTotalHit)
      val hitRate   = combHR._3/numUsers // #hit/#user.
      val precision = combHR._4/numUsers // precision => avg(#userHit/N)
  
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
      
      Map("Precision"-> precision, 
          "Recall"-> recall, 
          "AvgCombHitRate" -> hitRate,  
          "AvgTestHitRate" -> avgTestHR)
    }
}

/** metric type of recall for cold items recommendation**/
case class RecJobMetricColdRecall(metricName:String, metricParams: HashMap[String,
  String]) extends RecJobMetric {
    
  val IdenPrefix = "MetricColdRecall" 
      
  /**
   * @param topNPredColdItems RDD of topN predicted set and size of 
   * intersection with cold items
   */
  def run(topNPredColdItems:RDD[(Int, (List[String], Int))]):RecJobMetric.MetricResult = {
    Logger.info("topNPredColdItem" + topNPredColdItems.count)
    
    val recall =  if (topNPredColdItems.count == 0){
        //no data to compute: 
        //  this typically means there is no cold start entries. 
        //  and one possible bug is the feature extractor is not functional. 
        -1
    }else{
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
    
    
    
    Map("Recall" -> recall)
  }
}




/** Generic metric type of squared error */
trait RecJobMetricSE extends RecJobMetric {
	def run(labelNPred: RDD[(Double, Double)]): RecJobMetric.MetricResult
}

/** Metric type of mean squared error */
case class RecJobMetricMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
    
    val IdenPrefix = "MetricMSE"
    
	def run(labelNPred: RDD[(Double, Double)]): RecJobMetric.MetricResult = {
		val mse = ContinuousPrediction.computeMSE(labelNPred)
		Map("MSE" -> mse)
	}
}

/** Metric type of root mean squared error */
case class RecJobMetricRMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
    
    val IdenPrefix = "MetricRMSE"
    
    def run(labelNPred: RDD[(Double, Double)]): RecJobMetric.MetricResult = {
        val rmse = ContinuousPrediction.computeRMSE(labelNPred)
        Map("RMSE" -> rmse)
    }
}
