package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.GeneralizedLinearModelStruct
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.evaluation._
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.job.RecMatrixFactJob

/** Defines the type of test. */
sealed trait RecJobTest {
    /** The name of the test */
    def testName: String
    
    /** The parameters of the test */
    def testParams: HashMap[String, String]
	
    /** A list of metrics to be computed */
    def metricList: Array[RecJobMetric]
    
    /**
     * Runs model on test data and stores results in jobStatus
     */
	def run(jobInfo: RecJob, model:ModelStruct) = {
        val (testUnit, testReuslts) =
	        this match {
	        	case RecJobTestNoCold(testName, testParams, metricList) => 
	            	TestUnit.testNoCold(jobInfo, testParams, metricList, model)
	        	case RecJobTestColdItem(testName, testParams, metricList) => 
	        	    TestUnit.testColdItem(jobInfo, testParams, metricList, model)
	        	case _ => 
	        	    Logger.error("Test type not supported")
	        	    (null, new TestUnit.TestResults())
	        }
        
        if (!jobInfo.jobStatus.completedTests.isDefinedAt(model))
            jobInfo.jobStatus.completedTests(model) = new HashMap()
        val testMap = jobInfo.jobStatus.completedTests(model)
        
        if (testUnit != null){
        	testMap(testUnit) = testReuslts
        }
    }
    
    /**
     * Runs model on test data and stores results in jobStatus
     */
	def run(jobInfo: RecMatrixFactJob, model:MatrixFactModel) = {
        val (testUnit, testReuslts) =
	        this match {
	        	case RecJobTestNoCold(testName, testParams, metricList) => 
	            	TestUnit.testNoCold(jobInfo, testParams, metricList, model)
	        	case RecJobTestColdItem(testName, testParams, metricList) => 
	        	    TestUnit.testColdItem(jobInfo, testParams, metricList, model)
	        	case _ => 
	        	    Logger.error("Test type not supported")
	        	    (null, new TestUnit.TestResults())
	        }
        
        if (!jobInfo.jobStatus.completedTests.isDefinedAt(model))
            jobInfo.jobStatus.completedTests(model) = new HashMap()
        val testMap = jobInfo.jobStatus.completedTests(model)
        
        if (testUnit != null){
        	testMap(testUnit) = testReuslts
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
