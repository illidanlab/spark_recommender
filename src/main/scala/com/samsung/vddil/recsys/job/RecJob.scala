package com.samsung.vddil.recsys.job

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.text.SimpleDateFormat

import scala.Array.canBuildFrom
import scala.collection.mutable.HashMap
import scala.xml.Elem
import scala.xml.Node

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.job._
import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricRMSE
import com.samsung.vddil.recsys.feature.RecJobFactFeature
import com.samsung.vddil.recsys.feature.RecJobFeature
import com.samsung.vddil.recsys.feature.RecJobItemFeature
import com.samsung.vddil.recsys.feature.RecJobUserFeature
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.RecJobBinClassModel
import com.samsung.vddil.recsys.model.RecJobModel
import com.samsung.vddil.recsys.model.RecJobScoreRegModel
import com.samsung.vddil.recsys.model.SerializableModel
import com.samsung.vddil.recsys.prediction.RecJobPrediction
import com.samsung.vddil.recsys.testing.RecJobTest
import com.samsung.vddil.recsys.testing.RecJobTestColdItem
import com.samsung.vddil.recsys.testing.RecJobTestNoCold
import com.samsung.vddil.recsys.testing.getBestModel
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger

/**
 * The constant variables of recommendation job.
 * 
 * @author jiayu.zhou
 */
object RecJob{
	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	val ResourceLoc_JobTest    = "jobTest"
	val ResourceLoc_JobDir     = "job"
	    
	val ResourceLocAddon_GeoLoc = "geoLocation"
	    
	val DataSplitting_trainRatio = "trainRatio"
	val DataSplitting_testRatio  = "testRatio"
	val DataSplitting_validRatio = "validRatio"
	val DataSplitting_testRatio_default = 0.2
	val DataSplitting_validRatio_default = 0.1
	
	val SparkContext_master_default = "local[2]"
}

/**
 * The information (requirements) about a particular recommendation Job. 
 * 
 * @param jobName a name that will be display as well as construct job folder in the workspace. 
 * @param jobDesc a human readable job description 
 * @param jobNode a XML node of type scala.xml.Node, which will be used to parse all the job information.
 * 
 */
case class RecJob (jobName:String, jobDesc:String, jobNode:Node) extends JobWithFeature {
	//initialization 
    val jobType = JobType.Recommendation
    
    Logger.info("Parsing job ["+ jobName + "]")
    Logger.info("        job desc:"+ jobDesc)
    
    
    /** a list of models */
    val modelList:Array[RecJobModel] = populateMethods()
    
    /** a list of test procedures to be performed for each model */
    val testList:Array[RecJobTest] = populateTests()
    
    /** a list of experimental features. */ 
    val experimentalFeatures:HashMap[String, String] = populateExpFeatures()
    
    /**
     * Data splitting information 
     * 
     * {{{
     * val trainRatio:Double = dataSplit(RecJob.DataSplitting_trainRatio)
     * val testRatio:Double = dataSplit(RecJob.DataSplitting_testRatio)  
     * val trainRatio:Double = dataSplit(RecJob.DataSplitting_validRatio)
     * }}} 
     */
    val dataSplit:HashMap[String, Double] = populateDataSplitting()
    //TODO: parse and ensemble 
    
    /** the optional prediction phase*/
    val prediction:Option[RecJobPrediction] = populatePrediction()
    
    /** A data structure maintaining resources for intermediate results. */
    val jobStatus:RecJobStatus = new RecJobStatus(this)
    
    Logger.info("Job Parse done => " + this.toString)
    
    
    /** 
     *  Executes the main workflow of a recommender system job:
     *  
     *   1. Prepares (aggregates) training data
     *   
     *   2. Extracts features
     *   
     *   3. Learns models. Since each model can specify a different feature coverage threshold, 
     *                     each model learning involves an independent data assembling stage.
     *     
     *   4. Prepares testing data
     *   
     *   5. Carries out evaluations on testing data. 
     *  
     */
    def run():Unit= {
    	val logger = Logger.logger 

    	//Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	Logger.info("**preparing training data")
    	DataProcess.prepareTrain(this)
    	
    	//preparing features
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
	    	         modelUnit.run(this)
	    	     }
	    	}
    	}
    	
    	//testing recommendation performance on testing dates.
    	Logger.info("**preparing testing data")
    	DataProcess.prepareTest(this)
    	
    	Logger.info("**evaluating the models")
    	jobStatus.testWatchTime foreach { testData =>
    		//size of test data
    		Logger.info("Size of test data: " + testData.count)
    		
            //evaluate regression models on test data
    		Logger.info("Regression model num: " + jobStatus.resourceLocation_RegressModel.size)
    		jobStatus.resourceLocation_RegressModel.map{
    		    case (modelStr, model) =>
    		        Logger.info("Evaluating model: "+ modelStr)
    		        testList.map{_.run(this, model)}
    	 	}
    		
    		//evaluate classification models on test data
    		Logger.info("Classification model num: " + jobStatus.resourceLocation_ClassifyModel.size)
    		jobStatus.resourceLocation_ClassifyModel.map{
    		    case (modelStr, model) =>
    		        //TODO: evaluate classification models. 
    	 	}
        }
    	
    	Logger.info("Writing summary file")
    	writeSummaryFile()
    	
        Logger.info("Output prediction")
    	//pick the best model from completedTests and generate results
    	if (this.prediction.isDefined){
                Logger.info("Prediction module found.")

    		val bestModel = getBestModel(
    		        jobStatus.completedTests, 
    		        jobStatus.resourceLocation_RegressModel.values.toList ++
    		        jobStatus.resourceLocation_ClassifyModel.values.toList)
	    	bestModel.foreach{
	    	    theBestModel: ModelStruct => 
	    	        Logger.info("The best model obtained is "+ theBestModel)
	    	    prediction.get.run(this, theBestModel)
	    	}
    	}else{
            Logger.info("Prediction module not found.")
        }
    }
    
    
    /**
     * Does nothing for the moment. 
     */
    def generateXML():Option[Elem] = {
       None
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
     * Creates an instance of SparkContext
     * according to specification. 
     * 
     * @deprecated Use `Pipeline.instance.get.sc` to 
     * 		 get the instance of spark. 
     */
    def initSparkContext():Option[SparkContext]={
       var nodeList = jobNode \ JobTag.RecJobSparkContext
      
       var sparkContext_master:String  = RecJob.SparkContext_master_default
       // by default we use this.jobName as default job name of the spark context. 
       var sparkContext_jobName:String = this.jobName 
       
       if( nodeList.size > 0 && (nodeList(0) \JobTag.RecJobSparkContextMaster ).size > 0){
    	   sparkContext_master = (nodeList(0) \JobTag.RecJobSparkContextMaster).text 
       }else{
           Logger.warn("SparkContext specification not found, will try using local.")
       }
          
       if( nodeList.size > 0 && (nodeList(0) \JobTag.RecJobSparkContextJobName).size > 0){
    	   sparkContext_jobName = (nodeList(0) \JobTag.RecJobSparkContextJobName).text
       }  
   
       try{
          return Some(new SparkContext(sparkContext_master, sparkContext_jobName))
       }catch{
         case _:Throwable => Logger.error("Failed to build SparkContext!") 
       }
       
       None
    }
    
    /**
     * Populates data splitting information.
     * 
     * @return a map whose keys are given by 
     *    [[RecJob.DataSplitting_trainRatio]],
     *    [[RecJob.DataSplitting_testRatio]], and
     *    [[RecJob.DataSplitting_validRatio]]. 
     *    and values are double. 
     */
    def populateDataSplitting():HashMap[String, Double] = {
       var dataSplit:HashMap[String, Double] = new HashMap()
       
       var nodeList = jobNode \ JobTag.RecJobDataSplit
       if (nodeList.size > 0){
          //parse numbers when users have specified.
    	   if((nodeList(0) \ JobTag.RecJobDataSplitTestRatio).size > 0){
    	       try{
    	          dataSplit(RecJob.DataSplitting_testRatio) = ((nodeList(0) \ JobTag.RecJobDataSplitTestRatio).text).toDouble 
    	       }catch{ case _:Throwable => None}
    	   }
    	   
    	   if((nodeList(0) \ JobTag.RecJobDataSplitValidRatio).size > 0){
    	       try{
    	          dataSplit(RecJob.DataSplitting_validRatio) = ((nodeList(0) \ JobTag.RecJobDataSplitValidRatio).text).toDouble 
    	       }catch{ case _:Throwable => None}
    	   }
       }else{
          Logger.warn("Data splitting is not specified for job [%s]".format(jobName))
       }
       
       //use default if users have not specified we use default. 
       if(! dataSplit.isDefinedAt(JobTag.RecJobDataSplitTestRatio)) 
           dataSplit(RecJob.DataSplitting_testRatio) = RecJob.DataSplitting_testRatio_default
       
       if(! dataSplit.isDefinedAt(JobTag.RecJobDataSplitValidRatio))
           dataSplit(RecJob.DataSplitting_validRatio) = RecJob.DataSplitting_validRatio_default
       
       dataSplit(RecJob.DataSplitting_trainRatio) 
       	   = 1.0 - dataSplit(RecJob.DataSplitting_validRatio) - dataSplit(RecJob.DataSplitting_testRatio)
           
       dataSplit
    }
    
    /** Populate experimental features */
    def populateExpFeatures():HashMap[String, String] = {
        var expFeatures:HashMap[String, String] = HashMap()
        
        var nodeList = jobNode \ JobTag.RecJobExperimentalFeature
        if (nodeList.size == 0){
            Logger.info("No experimental features specified.")
            return expFeatures
        }
        
        for (expFeaturesNode <- nodeList){
            val expFeatureList = expFeaturesNode.child.
            		map(featureEntry => (featureEntry.label, featureEntry.text)).filter(_._1 != "#PCDATA")
            for(expFeaturePair <- expFeatureList){
                expFeatures += (expFeaturePair._1 -> expFeaturePair._2) 
            }
        }
        
        expFeatures
    }
    
    /** Populate prediction fields */
    def populatePrediction():Option[RecJobPrediction] = {
        
        var nodeList = jobNode \ JobTag.RecJobPred
        if (nodeList.size == 0){
            Logger.warn("No prediction specified.")
            return None
        }
        
        val parseNode = nodeList(0)
        
        //parse dates
        if ((parseNode \ JobTag.RecJobPredDateList).size <= 0){
            Logger.warn("INVALID PREDICTION: No dates found in prediction. ")
            return None
        }
        var dateList:Array[String] = (parseNode \ JobTag.RecJobPredDateList).map(_.text).
      			flatMap(expandDateList(_, dateParser)).  //expand the lists
      			toSet.toArray.sorted                     //remove duplication and sort.
        
        //parse content dates 
      	if ((parseNode \ JobTag.RecJobPredCntDate).size <= 0){
            Logger.warn("INVALID PREDICTION: No content dates found in prediction. ")
            return None
        }
        var contentDateList:Array[String] = (parseNode \ JobTag.RecJobPredCntDate).map(_.text).
      			flatMap(expandDateList(_, dateParser)).  //expand the lists
      			toSet.toArray.sorted                     //remove duplication and sort.
      			
      	//parse parameters 
      	val featureParam = parseNode \ JobTag.RecJobPredParam
        var paramList:HashMap[String, String] = HashMap()
        for (featureParamNode <- featureParam){
          // the #PCDATA is currently ignored. 
          val paraPairList = featureParamNode.child.map(feat => (feat.label, feat.text )).filter(_._1 != "#PCDATA")
          
          for (paraPair <- paraPairList){
            paramList += (paraPair._1 -> paraPair._2)
          }
        } 
        
        Some(RecJobPrediction(dateList, contentDateList, paramList))
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
    				case JobTag.RecJobTestType_NotCold   => testList = testList :+ RecJobTestNoCold  (testName, paramList, metricList)
    				case JobTag.RecJobTestType_ColdItems => testList = testList :+ RecJobTestColdItem(testName, paramList, metricList)
    				case _ => Logger.warn(s"Test type $testType not found or ignored.")
    			}
    		}
    		
    		
    	}
    	testList
    }
    
    
    /**
     * Populates learning methods from XML.
     * 
     *  @return a set of models to be learned
     */
    def populateMethods():Array[RecJobModel] = {
      var modelList:Array[RecJobModel] = Array ()
      
      var nodeList = jobNode \ JobTag.RecJobModelList
      if (nodeList.size == 0){
        Logger.warn("No models found!")
        return modelList
      }
      
      nodeList = nodeList(0) \ JobTag.RecJobModelUnit
      
      //populate each model. 
      for (node <- nodeList){
         val modelType = (node \ JobTag.RecJobModelUnitType).text
         
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
         
         //create model structs by type. 
         modelType match{
           case JobTag.RecJobModelType_Regress => modelList = modelList :+ RecJobScoreRegModel(modelName, paramList)
           case JobTag.RecJobModelType_Classify => modelList = modelList :+ RecJobBinClassModel(modelName, paramList)
           case _ => Logger.warn(s"Model type $modelType not found and ignored.")
         }
      }
      
      //TODO: if there are multiple models, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       s"Job [Recommendation][${this.jobName}][${this.trainDates.length} dates][${this.featureList.length} features][${this.modelList.length} models]"
    }
    
    /**
     * Return the current job status. 
     */
    def getStatus():JobStatus = {
       return this.jobStatus
    }
    
}

/** 
 *  a compact class to represent rating
 *  
 *   @param user the mapped index of user
 *   @param item the mapped index of item
 *   @param rating the value of rating 
 */
case class Rating(user: Int, item: Int, rating: Double)
