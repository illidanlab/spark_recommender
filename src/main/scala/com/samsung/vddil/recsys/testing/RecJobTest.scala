package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.GeneralizedLinearModelStruct
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.evaluation.RecJobMetricSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR

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
		
		model match {
			//get predicted labels
			case linearModel:GeneralizedLinearModelStruct => {
			    metricList.map { metric =>
			    	metric match {
			    		case metricSE:RecJobMetricSE => {
			    			linearModelSEEval(jobInfo, linearModel)
			    			testHandlerRes foreach { testVal =>
		    				     val score = metricSE.run(testVal.map{x => (x._3, x._4)})
		    				     //TODO: add test type
		    				     Logger.info(s"Evaluated $model Not Coldstart $metric = $score")
			    				
			    			}
			    		}
			    		
			    		case metricHR:RecJobMetricHR => {
			    			linearModelHREval(jobInfo, linearModel)
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
			case _ => None
		}
	}
	
	def linearModelSEEval(jobInfo: RecJob, 
			                linearModel:GeneralizedLinearModelStruct) = {
		if (!testHandlerRes.isDefined) {
            testHandlerRes = Some(LinearRegNotColdTestHandler.performTest(jobInfo, 
            		                          testName, testParams, linearModel))
        }
	}	
	
	def linearModelHREval(jobInfo: RecJob, 
			                linearModel:GeneralizedLinearModelStruct) = {
		if (!hitTestHandlerRes.isDefined) {
			hitTestHandlerRes = Some(RegNotColdHitTestHandler.performTest(jobInfo, 
					                    testName, testParams, linearModel))
		}
	}
	
}