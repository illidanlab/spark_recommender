package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.evaluation.RecJobMetricSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall

/**
 * The test unit, each test should implement this trait and create a factory method 
 * in the TestUnit companion object. 
 */
trait TestUnit {
    
    def testParams: HashMap[String, String]
    
    def metricList: Array[RecJobMetric]
    
    /** A prefix string */
    def IdenPrefix:String
    
    /** Generates the unique resource string for a particular test */
	def resourceIdentity():String = {
        IdenPrefix + "_" + 
        		HashString.generateHash(testParams.toString)
    }
    
    /**
     * Run tests on different metrics
     */
    def performTest(jobInfo: RecJob, model:ModelStruct):TestUnit.TestResults
}

/**
 * Factory methods for performing tests. 
 */
object TestUnit{
    /**
     * Each test contains a set of metrics, and each metric can give 
     * a set of numbers. 
     * 
     * For example ["Precision", 0.4] and ["Recall", 0.4]
     */
    type TestResults = HashMap[RecJobMetric, Map[String, Double]]  
    
    /**
     * Perform test without cold start
     */
    def testNoCold(
            jobInfo:RecJob, 
            testParams:HashMap[String, String],  
            metricList: Array[RecJobMetric], 
            model:ModelStruct):TestResults = {
        new TestUnitNoCold(testParams, metricList).performTest(jobInfo, model)
    }
    
    /**
     * Perform test on cold items
     */
    def testColdItem(
            jobInfo:RecJob, 
            testParams:HashMap[String, String],  
            metricList: Array[RecJobMetric], 
            model:ModelStruct):TestResults = {
        new TestUnitColdItem(testParams, metricList).performTest(jobInfo, model)
    }
}


case class TestUnitNoCold(
        	testParams:HashMap[String, String],  
        	metricList: Array[RecJobMetric]
        ) extends TestUnit{
     
    val IdenPrefix = "Test_NoColdStart"
        
    def performTest(jobInfo: RecJob, model:ModelStruct):TestUnit.TestResults = {
        
        //1. the test result to be returned. 
        val result:TestUnit.TestResults = new TestUnit.TestResults()
        				
        Logger.info("Performing NoCold test on model: " + model.resourceStr)
        
        //2. 
        //work directories 
        val testDir = jobInfo.resourceLoc(RecJob.ResourceLoc_JobTest) + 
        		"/" + model.resourceStr + "/" + this.resourceIdentity
        //the directory in which the temporary resources related to tests are stored. 
        val testResourceDir = testDir + "/resources"
        		
        //3. A list of resources to be reused throughout the metrics
        //squared error
		var testHandlerRes:Option[RDD[(Int, Int, Double, Double)]] = None
		//hit ranking
		var hitTestHandlerRes:Option[RDD[HitSet]] = None
		
		//4. Run each metric. 
		metricList.map { metric =>
	    		
	    	metric match {
	    		case metricSE:RecJobMetricSE => {
	    		    //1 Generate necessary resources. 
	    		    //The squared error (SE) requires the computation of numerical prediction. 
	    			if (!testHandlerRes.isDefined){
	    			    //Runs model on test data and return RDD of (user, item, actual label, predicted label) 
	    			    testHandlerRes = Some(TestResourceLinearRegNotCold.generateResource(jobInfo, 
    		                           testParams, model, testResourceDir))
	    			}
	    			
	    			//2. Compute metric scores. 
	    			val score = metricSE.run(testHandlerRes.get.map{x => (x._3, x._4)})
	    			
	    			//3. Save 
	    			result(metricSE) = Map("SquaredError" -> score)

    				Logger.info(s"Evaluated $model Not Coldstart $metric = $score")
	    		}
	    		
	    		case metricHR:RecJobMetricHR => {
	    		    //TODO: check if results are available, if so load it. 
	    		    
	    			//1 Generate necessary resources.
	    			if (!hitTestHandlerRes.isDefined){
	    			    hitTestHandlerRes = Some(TestResourceRegNotColdHit.generateResource(jobInfo, 
			                     		  testParams, model, testResourceDir))
	    			}
	    			
	    			val scores  = metricHR.run(hitTestHandlerRes.get)
	    			result(metricHR) = Map("avgCombHR" -> scores._1, "avgTestHR" -> scores._2)
	    			
                    Logger.info(s"Evaluated $model Not Coldstart  $metric = $scores")
	    		}
	    		
	    		case _ => Logger.warn(s"$metric not known metric")
	    	}
		}
        
        //5. Save the results. 
        result
    }
}

case class TestUnitColdItem(
        	testParams:HashMap[String, String],  
        	metricList: Array[RecJobMetric]
        ) extends TestUnit{
    
    val IdenPrefix = "Test_ColdItem"
    
    def performTest(jobInfo: RecJob, model:ModelStruct):TestUnit.TestResults = {
        //the results to be returned. 
        val result:TestUnit.TestResults = new TestUnit.TestResults()
        
        Logger.info("Performing NoCold test on model: " + model.resourceStr)
        
        val testResourceDir = jobInfo.resourceLoc(RecJob.ResourceLoc_JobTest) + 
        		"/" + model.resourceStr + "/" + this.resourceIdentity + "/resources"
        
        var coldItemTestResource:Option[RDD[(Int, (List[String], Int))]] = None
        
        metricList.map {metric =>
            metric match{
                case metricRecall:RecJobMetricColdRecall => {
                	
                	if (!coldItemTestResource.isDefined) {
			    		coldItemTestResource =
			    				Some(TestResourceRegItemColdHit.generateResource(jobInfo,
			    						testParams, model, testResourceDir))
			    	}                	
                	
                	val scores = metricRecall.run(coldItemTestResource.get)
                	result(metricRecall) = Map("ColdStartHitRate"->scores)
                	
                	Logger.info(s"Evaluated $model coldstart $metric = $scores")
                }
                
                case _ => Logger.warn(s"$metric not known for cold item")
            }
        }
        
        result
    }
}



