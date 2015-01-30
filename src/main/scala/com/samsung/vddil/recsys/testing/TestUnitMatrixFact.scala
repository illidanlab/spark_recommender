package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.Path
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.evaluation.RecJobMetricSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileSystem
import scala.io.Source
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import com.samsung.vddil.recsys.job.RecJob

/**
 * The test unit, each test should implement this trait and create a factory method 
 * in the TestUnit companion object. 
 */
trait TestUnitMatrixFact {
    
    def testParams: HashMap[String, String]
    
    def metricList: Array[RecJobMetric]
    
    def model:MatrixFactModel
    
    def jobInfo:RecMatrixFactJob
    
    /** A prefix string */
    def IdenPrefix:String
    
    /** Generates the unique resource string for a particular test */
	def resourceIdentity():String = {
        IdenPrefix + "_" + 
        		HashString.generateHash(testParams.toString)
    }
    
    /** Run tests on different metrics */
    def performTest():TestUnit.TestResults
    
    /** Returns the directory in which the results are to be stored. */
    def testDirectory():String = {
        jobInfo.resourceLoc(RecJob.ResourceLoc_JobTest) + 
        		"/" + model.resourceStr + "/" + this.resourceIdentity
    }
    
    /** Returns the directory in which the temporary resources related to tests are stored. */
    def testResourceDirectory():String = {
        testDirectory + "/resources" 
    }
    
    /** Returns the metric result location for a specific model and this test. */
    def testMetricFile(metric:RecJobMetric):String = {
        testDirectory  + "/Metric_" + metric.resourceIdentity 
    }
    
    /** Write plain text summary file*/
    def writeSummaryFile(results:TestUnit.TestResults){
        
    	val summaryFileStr = 
        	  jobInfo.resourceLoc(RecJob.ResourceLoc_JobTest) + 
        		"/" + model.resourceStr + "/Summary_" + this.resourceIdentity
        		
        val summaryFile = new Path(summaryFileStr)
        
        //remove the previous one if found. 
        //may consider rename it later on. 
        val fs = Pipeline.instance.get.fs
        if (fs.exists(summaryFile)) fs.delete(summaryFile, true)
        
        TestUnitMatrixFact.writeSummaryFile(fs, summaryFile, results, this)
    }
}

/**
 * Factory methods for performing tests. 
 */
object TestUnitMatrixFact{
    /**
     * Each test contains a set of metrics, and each metric can give 
     * a set of numbers. 
     * 
     * For example ["Precision", 0.4] and ["Recall", 0.4]
     */
    type TestResults = HashMap[RecJobMetric, RecJobMetric.MetricResult]  
    
    /**
     * Perform test without cold start
     */
    def testNoCold(
            jobInfo:RecMatrixFactJob, 
            testParams:HashMap[String, String],  
            metricList: Array[RecJobMetric], 
            model:MatrixFactModel):(TestUnitMatrixFact, TestResults) = {
        val test = new TestUnitNoColdMatrixFact(testParams, metricList,jobInfo, model)
        (test, test.performTest())
    }
    
    /**
     * Perform test on cold items
     */
    def testColdItem(
            jobInfo:RecMatrixFactJob, 
            testParams:HashMap[String, String],  
            metricList: Array[RecJobMetric], 
            model:MatrixFactModel):(TestUnitMatrixFact, TestResults) = {
        val test = new TestUnitColdItemMatrixFact(testParams, metricList,jobInfo, model) 
        (test, test.performTest())
    }
    
    /**
     * Writes the plain text summary file to target file. 
     * 
     * This method assumes the summaryFile does not exist. 
     */
    def writeSummaryFile(
            fs:FileSystem, 
            summaryFile:Path, 
            results:TestResults, 
            testUnit:TestUnitMatrixFact){
        
        val out = fs.create(summaryFile);
        val writer = new BufferedWriter(new OutputStreamWriter(out))
        
        //write 
        writer.write("===Test Unit Summary START==="); writer.newLine()
        writer.write("Test Unit Class:             " + testUnit.getClass().getName()); writer.newLine()
        writer.write("Test Unit Resource Identity: " + testUnit.resourceIdentity); writer.newLine()
        writer.write("Test Unit Parameters:        " + testUnit.testParams.toString); writer.newLine()
        writer.write("Model:                       " + testUnit.model.resourceStr); writer.newLine()
        writer.write("Model Param:                 " + testUnit.model.modelParams.toString); writer.newLine()
        
        writer.write("Metric List:                 "); writer.newLine()
        for((metric, metricResult )<- results){
            writer.write("  Metric Resource Identity: " + metric.resourceIdentity); writer.newLine()
            writer.write("  Metric Parameters:        " + metric.metricParams.toString); writer.newLine()
            for((resultStr, resultVal) <- metricResult) {
                writer.write("    [" + resultStr + "] " + resultVal.formatted("%.4g")); writer.newLine()
            }
        }
        
        writer.write("===Test Unit Summary END==="); writer.newLine()
        writer.close()
        
        out.close()
    }
}


case class TestUnitNoColdMatrixFact private[testing] (
        	testParams:HashMap[String, String],  
        	metricList: Array[RecJobMetric],
        	jobInfo: RecMatrixFactJob,
        	model:MatrixFactModel
        ) extends TestUnitMatrixFact{
     
    val IdenPrefix = "Test_NoColdStart"
        
    def performTest():TestUnit.TestResults = {
        Logger.info("Performing NoCold test on model: " + model.resourceStr)
        
        
        //1. the test result to be returned. 
        val result:TestUnit.TestResults = new TestUnit.TestResults()
        
        //2. Set up directories. 
        val testDir         = testDirectory
        val testResourceDir = testResourceDirectory
        		
        //3. A list of resources to be reused throughout the metrics
		var testHandlerRes:Option[RDD[(Int, Int, Double, Double)]] = None   //squared error
		var hitTestHandlerRes:Option[RDD[HitSet]] = None                    //hit ranking
		
		//4. Run each metric. 
		metricList.map { metric =>
		    
	        val metricFile:String = testMetricFile(metric)
	        //see if we need to recompute the metric results. 
	        if(jobInfo.outputResource(metricFile)){
	            //recompute the metric. 
		    	val computedMetricResult: RecJobMetric.MetricResult = metric match {
		    	    
		    		case metricSE:RecJobMetricSE => {	    
		    		    //1 Generate necessary resources. 
		    		    //The squared error (SE) requires the computation of numerical prediction. 
		    			if (!testHandlerRes.isDefined){
		    			    //Runs model on test data and return RDD of (user, item, actual label, predicted label) 
		    			    testHandlerRes = Some(TestResourceLinearRegNotCold.generateResource(jobInfo, 
	    		                           testParams, model, testResourceDir))
		    			}
		    			
		    			//2. Compute metric scores. 
		    			val metricResult = metricSE.run(testHandlerRes.get.map{x => (x._3, x._4)})
		    			Logger.info(s"Evaluated $model Not Coldstart $metric = $metricResult")
		    			
		    			//3. Save 
		    			metricResult
		    		}
		    		
		    		case metricHR:RecJobMetricHR => {
		    			//1 Generate necessary resources.
//		    			if (!hitTestHandlerRes.isDefined){
//		    			    hitTestHandlerRes = Some(TestResourceRegNotColdHit.generateResource(jobInfo, 
//				                     		  testParams, model, testResourceDir))
//		    			}
		    			
		    		    val testResource = TestResourceRegNotColdHit.generateResource(jobInfo, 
				                     		  testParams, model, testResourceDir)
		    		    
		    			//2. Compute metric scores. 
		    			val metricResult = metricHR.run(testResource)
		    			Logger.info(s"Evaluated $model Not Coldstart  $metric = $metricResult")
		    			
		    			//3. Save 
		    			metricResult    
		    		}
		    		
		    		case _ => 
		    		    Logger.warn(s"$metric not known metric")
		    		    RecJobMetric.emptyResult
		    	}
		    	//save the metric result file. 
		    	RecJobMetric.saveMetricResult(metricFile, computedMetricResult)
	        }
	        //read metric result file.
	        result(metric) = RecJobMetric.loadMetricResult(metricFile)
		}
        //5. Write summary file. 
        writeSummaryFile(result)
        
        //6. Return the results. 
        result
    }
}

case class TestUnitColdItemMatrixFact private[testing](
        	testParams:HashMap[String, String],  
        	metricList: Array[RecJobMetric],
        	jobInfo: RecMatrixFactJob,
        	model:MatrixFactModel
        ) extends TestUnitMatrixFact{
    
    val IdenPrefix = "Test_ColdItem"
    
    def performTest():TestUnit.TestResults = {
        Logger.info("Performing Cold test on model: " + model.resourceStr)
        
        //1. the results to be returned. 
        val result:TestUnit.TestResults = new TestUnit.TestResults()
        
        //2. Set up directories. 
        val testDir         = testDirectory
        val testResourceDir = testResourceDirectory
        
        //3. Resource variables. 
        var coldItemTestResource:Option[RDD[(Int, (List[String], Int))]] = None
        
        //4. Run each metric. 
        metricList.map {metric =>
            
            val metricFile:String = testMetricFile(metric)
            //see if we need to recompute the metric results. 
	        if(jobInfo.outputResource(metricFile)){
	            val computedMetricResult: RecJobMetric.MetricResult = metric match {
	                case metricRecall:RecJobMetricColdRecall => {
	                	
	                	if (!coldItemTestResource.isDefined) {
				    		coldItemTestResource =
				    				Some(TestResourceRegItemColdHit.generateResource(jobInfo,
				    						testParams, model, testResourceDir))
				    	}                	
	                	
	                	val metricResult = metricRecall.run(coldItemTestResource.get)
	                	Logger.info(s"Evaluated $model coldstart $metric = $metricResult")
	                	
	                	metricResult
	                }
	                case _ => 
	                    Logger.warn(s"$metric not known for cold item")
	                    RecJobMetric.emptyResult
	            }
	            //save the metric result file. 
		    	RecJobMetric.saveMetricResult(metricFile, computedMetricResult)
	        }
            //read metric result file.
	        result(metric) = RecJobMetric.loadMetricResult(metricFile)
        }
        
        //5. Write summary file.  
        writeSummaryFile(result)
        
        //6. Return the results.
        result
    }
}


