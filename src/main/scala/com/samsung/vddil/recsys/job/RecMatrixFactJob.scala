package com.samsung.vddil.recsys.job

import scala.xml.Node
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import com.samsung.vddil.recsys.Pipeline
import java.text.SimpleDateFormat
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.testing.RecJobTest
import com.samsung.vddil.recsys.testing.RecJobTestNoCold
import com.samsung.vddil.recsys.testing.RecJobTestColdItem
import com.samsung.vddil.recsys.evaluation.RecJobMetricMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricRMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall
import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.mfmodel.MatrixFactModelHandler
import com.samsung.vddil.recsys.feature.RecJobFeature
import org.apache.hadoop.fs.Path
import java.io.BufferedWriter
import java.io.OutputStreamWriter


object RecMatrixFactJob{
	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	val ResourceLoc_JobTest    = "jobTest"
	val ResourceLoc_JobDir     = "job"
	   
	
}


case class RecMatrixFactJob(jobName:String, jobDesc:String, jobNode:Node) extends JobWithFeature {
	//initialization 
    val jobType = JobType.RecMatrixFact
    
    Logger.info("Parsing job ["+ jobName + "]")
    Logger.info("        job desc:"+ jobDesc)
   
    
    /** a list of models */
    val modelList:Array[RecMatrixFactJobModel] = populateMethods()
    
    /** a list of test procedures to be performed for each model */
    val testList:Array[RecJobTest] = populateTests()
    
    /** A data structure maintaining resources for intermediate results. */
    val jobStatus:RecMatrixFactStatus = new RecMatrixFactStatus(this)
    
    
    Logger.info("Job Parse done => " + this.toString)
    
    /** 
     *  Executes the main workflow of a matrix factorization-based recommender system job:
     *  
     *   1. Prepares (aggregates) training data
     *   
     *   2. Learns models. 
     *     
     *   3. Prepares testing data
     */    
    def run():Unit = {
        val logger = Logger.logger
        
        //Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	Logger.info("**preparing training data")
    	DataProcess.prepareTrain(this)
    	
    	//Prepare features in case some matrix factorization algorithms may use for cold start.
    	Logger.info("**preparing features")
    	//   for each feature, we generate the resource  
    	this.featureList.foreach{
    		featureUnit =>{
    		    Logger.info("*preparing features" + featureUnit.toString())
    		    featureUnit.run(this)
    		    //status: update Job status
    		}
    	} 
    	
    	//learning models
    	if (this.modelList.length > 0){
    		  
    		Logger.info("**learning models")
	    	this.modelList.foreach{
	    	     modelUnit => {
	    	         Logger.info("*buildling model" + modelUnit.toString())
	    	         val modelOption = modelUnit.run(this)
	    	         if (modelOption.isDefined) {
	    	             this.jobStatus.resourceLocation_models = 
	    	                 this.jobStatus.resourceLocation_models + modelOption.get
	    	         }
	    	     }
	    	}
    	}
        
        //testing recommendation performance on testing dates.
    	Logger.info("**preparing testing data")
        DataProcess.prepareTest(this)
    	
        jobStatus.resourceLocation_models.map{
    	    case(modelStr, model) => 
    	        Logger.info("Evaluating model: "+ modelStr)
    	        testList.map{_.run(this, model)}
    	}
        
    	writeSummaryFile()
    	
    }
    /**
     * Generates a summary file under the job workspace folder.  
     */
    def writeSummaryFile(){
        val summaryFile = new Path(resourceLoc(RecJob.ResourceLoc_JobDir) + "/Summary.txt")
        
        //always overwrite existing summary file. 
        if (fs.exists(summaryFile)) fs.delete(summaryFile, true)
        
        val out = fs.create(summaryFile)
        val writer = new BufferedWriter(new OutputStreamWriter(out))
        
        outputSummaryFile(this, writer)

        //clean
        writer.close()
        out.close()
    }
    
    /**
     * Populates test information from XML
     * 
     * @return a set of tests to be done in the evaluation stage
     */
    def populateTests():Array[RecJobTest] = {
    	var testList:Array[RecJobTest] = Array()
    	var nodeList = jobNode \ JobTag.RecJobTestList
    	if (nodeList.size == 0){
    	    Logger.warn("No tests found!")
    	    testList
    	} else {
    		nodeList = nodeList(0) \ JobTag.RecJobTestUnit
    		
    		//populate each test
    		for (node <- nodeList) {
    			val testType = (node \ JobTag.RecJobTestUnitType).text
    			val testName = (node \ JobTag.RecJobTestUnitName).text
    			val testParam = node \ JobTag.RecJobTestUnitParam
    			var paramList:HashMap[String, String] = HashMap()
    			
    			val testMetricNode = node \ JobTag.RecJobMetricList
    			
    			// a list of test metrics to be used in test procedures 
    			val metricList:Array[RecJobMetric] = if (testMetricNode.size == 0){
    			    Array()
    			}else{
    				populateMetric(testMetricNode(0))
    				testMetricNode.flatMap{metricNode => populateMetric(metricNode)}.toArray
    			}
    			
    			//populate test parameters
    			for (param <- testParam) {
    			    val paraPairList = param.child
    			                            .map(line => (line.label, line.text))
    			                            .filter(_._1 != "#PCDATA")
    			    for (paraPair <- paraPairList) {
    			    	paramList += (paraPair._1 -> paraPair._2)
    			    }
    			}
    		
    		    //create tests by type
    			testType match {
    				case JobTag.RecJobTestType_NotCold   => 
    				    testList = testList :+ RecJobTestNoCold  (testName, paramList, metricList)
    				case JobTag.RecJobTestType_ColdItems => 
    				    testList = testList :+ RecJobTestColdItem(testName, paramList, metricList)
    				case _ => Logger.warn(s"Test type $testType not found or ignored.")
    			}
    		}
    		
    		
    	}
    	testList
    }   
    
    /**
     * Populates required evaluation metrics from XML
     * 
     * @return a set of metrics to be computed in evaluation. 
     */
    def populateMetric( node:Node ):Array[RecJobMetric] = {
    	var metricList:Array[RecJobMetric] = Array()
    	var nodeList = node \ JobTag.RecJobMetricUnit
    	if (nodeList.size == 0) {
            Logger.warn("No metrics found!")
            metricList
        } else {
        	
        	//populate each metric
        	for (node <- nodeList) {
        	    val metricType = (node \ JobTag.RecJobMetricUnitType).text
                val metricName = (node \ JobTag.RecJobMetricUnitName).text
                val metricParam = node \ JobTag.RecJobMetricUnitParam
                var paramList:HashMap[String, String] = HashMap()
                
                //populate metric parameters
                for (param <- metricParam) {
                	val paraPairList = param.child
                                            .map(line => (line.label, line.text))
                                            .filter(_._1 != "#PCDATA")
                    for (paraPair <- paraPairList) {
                        paramList += (paraPair._1 -> paraPair._2)
                    }                      
                }
        	    
        	    //create metrics by type
        	    metricType match {
        	    	case JobTag.RecJobMetricType_MSE        => metricList = metricList :+ RecJobMetricMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_RMSE       => metricList = metricList :+ RecJobMetricRMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_HR         => metricList = metricList :+ RecJobMetricHR(metricName, paramList)
        	    	case JobTag.RecJobMetricType_ColdRecall => metricList = metricList :+ RecJobMetricColdRecall(metricName, paramList)
        	    	case _ => Logger.warn(s"Metric type $metricType not found or ignored.")
        	    }
        	}
        }
    	
    	metricList
    }
    
    def generateXML(): Option[scala.xml.Elem] = None
    
    def getStatus(): com.samsung.vddil.recsys.job.JobStatus = {
        this.jobStatus
    }
    
        /**
     * Populates learning methods from XML.
     * 
     *  @return a set of models to be learned
     */
    def populateMethods():Array[RecMatrixFactJobModel] = {
      var modelList:Array[RecMatrixFactJobModel] = Array ()
      
      var nodeList = jobNode \ JobTag.RecJobModelList
      if (nodeList.size == 0){
        Logger.warn("No models found!")
        return modelList
      }
      
      nodeList = nodeList(0) \ JobTag.RecJobModelUnit
      
      //populate each model. 
      for (node <- nodeList){
         //val modelType = (node \ JobTag.RecJobModelUnitType).text
         
         val modelName = (node \ JobTag.RecJobModelUnitName).text
         
         val modelParam = node \ JobTag.RecJobModelUnitParam
         
         var paramList:HashMap[String, String] = HashMap()
         
         //populate model parameters
         for (featureParamNode <- modelParam){
           
           val paraPairList = featureParamNode.child.map(line => (line.label, line.text)).filter(_._1 != "#PCDATA")
           
           for (paraPair <- paraPairList){
             paramList += (paraPair._1 -> paraPair._2)
           }
         }
         
         //create model structures by type. 
         modelList = modelList :+ RecMatrixFactJobModel(modelName, paramList)
         
      }
      
      //TODO: if there are multiple models, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       s"Job [MatrixFact Recommendation][${this.jobName}][${this.trainDates.length} dates][${this.featureList.length} features][${this.modelList.length} models]"
    }
    
}


case class RecMatrixFactJobModel(
        val modelName:String, 
        val modelParams: HashMap[String, String]){
    def run(jobInfo:RecMatrixFactJob): Option[(String, MatrixFactModel)] = {
        
        //obtain rating data
        val ratingData:CombinedDataSet 
        	= jobInfo.jobStatus.resourceLocation_CombinedData_train.get
        
        //build model and return.  
        val modelOption = MatrixFactModelHandler.buildModel(modelName, modelParams, ratingData, jobInfo)
        
        //return results. 
        if(modelOption.isDefined){
            val model = modelOption.get
            Some((model.resourceStr, model))
        }else{
            None
        }
    }
}