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
	
    /** A list of metrics to be computed */
    def metricList: Array[RecJobMetric]
    
    /**
     * Runs model on test data and returns results in the form 
     * of (user, item, actual label, predicted label)
     * 
     * @return RDD of (user, item, actual label, predicted label)
     */
	def run(jobInfo: RecJob, model:ModelStruct):TestUnit.TestResults = {
        this match {
        	case RecJobTestNoCold(testName, testParams, metricList) => 
            	TestUnit.testNoCold(jobInfo, testParams, metricList, model)
        	case RecJobTestColdItem(testName, testParams, metricList) => 
        	    TestUnit.testColdItem(jobInfo, testParams, metricList, model)
        	case _ => 
        	    Logger.error("Test type not supported")
        	    new TestUnit.TestResults()
        }
    }
}

/** 
 *  Non cold-start evaluation 
 *  
 *  Supported metrics: RecJobMetricSE, RecJobMetricHR
 */
case class RecJobTestNoCold(
        testName: String, 
        testParams: HashMap[String, String], 
        metricList: Array[RecJobMetric]) 
    extends RecJobTest 

/**  
 *  Item cold-start evaluation
 *  
 *  Supported metrics: RecJobMetricColdRecall
 */
case class RecJobTestColdItem(
        testName:String, 
        testParams: HashMap[String, String],
        metricList: Array[RecJobMetric]) 
    extends RecJobTest
