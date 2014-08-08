package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.GeneralizedLinearModelStruct
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.evaluation._

/** Defines the type of test. */
sealed trait RecJobTest {
    /** The name of the test */
    def testName: String
    
    /** The parameters of the test */
    def testParams: HashMap[String, String]
	
    /**
     * Runs model on test data and returns results in the form 
     * of (user, item, actual label, predicted label)
     * 
     * @return RDD of (user, item, actual label, predicted label)
     */
	def run(jobInfo: RecJob, model:ModelStruct, metricList:Array[RecJobMetric])
}

/** Non cold start evaluation */
case class RecJobTestNoCold(testName: String, testParams: HashMap[String, String]) 
    extends RecJobTest {
	
	var testHandlerRes:Option[RDD[(Int, Int, Double, Double)]] = None
	var hitTestHandlerRes:Option[RDD[HitSet]] = None
	
	/*
	 * run model on test data and return RDD of (user, item, actual label, predicted label)
	 */
	def run(jobInfo: RecJob, model:ModelStruct, metricList:Array[RecJobMetric]) = {
		
		metricList.map { metric =>
			    	metric match {
			    		case metricSE:RecJobMetricSE => {
			    			linearModelSEEval(jobInfo, model, metric.metricParams)
			    			testHandlerRes foreach { testVal =>
		    				     val score = metricSE.run(testVal.map{x => (x._3, x._4)})
		    				     //TODO: add test type
		    				     Logger.info(s"Evaluated $model Not Coldstart $metric = $score")
			    				
			    			}
			    		}
			    		
			    		case metricHR:RecJobMetricHR => {
			    			linearModelHREval(jobInfo, model, metric.metricParams)
			    			hitTestHandlerRes foreach { testVal =>
			    				val scores  = metricHR.run(testVal)
			    				//TODO: add test type
                                 Logger.info(s"Evaluated $model Not Coldstart  $metric = $scores")
			    			}
			    		}
			    		
			    		case _ => Logger.warn(s"$metric not known metric")
			    	}
			    }
	}
	
	def linearModelSEEval(jobInfo: RecJob, 
			                linearModel:ModelStruct,
			                metricParams:HashMap[String, String]) = {
		if (!testHandlerRes.isDefined) {
            testHandlerRes = Some(LinearRegNotColdTestHandler.performTest(jobInfo, 
            		                          testName, testParams, metricParams, linearModel))
        }
	}	
	
	def linearModelHREval(jobInfo: RecJob, 
			                linearModel:ModelStruct, 
			                metricParams:HashMap[String, String]) = {
		if (!hitTestHandlerRes.isDefined) {
			hitTestHandlerRes = Some(RegNotColdHitTestHandler.performTest(jobInfo, 
					                    testName, testParams, metricParams, linearModel))
		}
	}
	
}

/** Item coldstart evaluation **/
case class RecJobTestColdItem(testName:String, testParams: HashMap[String,
  String]) extends RecJobTest {
 
  var coldItemTestHandlerRes:Option[RDD[(Int, (List[String], Int))]] = None
  
  def run(jobInfo:RecJob, model:ModelStruct, metricList:Array[RecJobMetric]) = {
    metricList.map {metric =>
      metric match{
        case metricRecall:RecJobMetricColdRecall => {
          modelRecallEval(jobInfo, model, metric.metricParams)
          coldItemTestHandlerRes foreach { testVal =>
            val scores = metricRecall.run(testVal)
            Logger.info(s"Evaluated $model coldstart $metric = $scores")
          }
        }
        case _ => Logger.warn(s"$metric not known for cold item")
      }
    }
  }

  def modelRecallEval(jobInfo:RecJob, model:ModelStruct, 
    metricParams: HashMap[String, String]) = {
    if (!coldItemTestHandlerRes.isDefined) {
      coldItemTestHandlerRes =
        Some(RegItemColdHitTestHandler.performTest(jobInfo, testName,
          testParams, metricParams, model))
    }
  }
  
}
