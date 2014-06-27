/**
 * The recommendation Job
 * 
 * @author jiayu.zhou
 * 
 */

package com.samsung.vddil.recsys.job

import scala.xml._
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext

import com.samsung.vddil.recsys.Logger

import com.samsung.vddil.recsys.feature.ItemFeatureHandler
import com.samsung.vddil.recsys.feature.UserFeatureHandler
import com.samsung.vddil.recsys.feature.FactFeatureHandler

import com.samsung.vddil.recsys.data.DataProcess
import com.samsung.vddil.recsys.data.DataAssemble
import com.samsung.vddil.recsys.data.DataSplitting

import com.samsung.vddil.recsys.evaluation.ContinuousPrediction


import com.samsung.vddil.recsys.model._

import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString

import com.samsung.vddil.recsys.testing.TestingHandler
import com.samsung.vddil.recsys.testing.LinearRegNotColdTestHandler


object RecJob{
	val ResourceLoc_RoviHQ     = "roviHq"
	val ResourceLoc_WatchTime  = "watchTime"
	val ResourceLoc_Workspace  = "workspace"
	val ResourceLoc_JobFeature = "jobFeature"
	val ResourceLoc_JobData    = "jobData"
	val ResourceLoc_JobModel   = "jobModel"
	  
	val DataSplitting_trainRatio = "trainRatio"
	val DataSplitting_testRatio  = "testRatio"
	val DataSplitting_validRatio = "validRatio"
	val DataSplitting_testRatio_default = 0.2
	val DataSplitting_validRatio_default = 0.1
	
	val SparkContext_master_default = "local[2]"
}

/**
 * The information about a particular recommendation Job. 
 * 
 * jobName: a name that will be display as well as construct job folder in the workspace. 
 * jobDesc: a human readable job description 
 * jobNode: a XML node of type scala.xml.Node, which will be used to parse all the job information.
 * 
 * derived fields.
 * 
 * resourceLoc: this is used to store resource location
 * 				INPUT RESOURCE
 *  				resourceLoc(RecJob.ResourceLoc_RoviHQ):String     ROVI data folder
 *  				resourceLoc(RecJob.ResourceLoc_WatchTime):String  ACR data folder
 *     			OUTPUT RESOURCE
 *  				resourceLoc(RecJob.ResourceLoc_JobFeature):String store features for this job 
 *  				resourceLoc(RecJob.ResourceLoc_JobData):String    store data for this job 
 *  				resourceLoc(RecJob.ResourceLoc_JobModel):String   store model for this job
 * 
 * featureList: a list of features. 
 * 
 * modelList: a list of models
 * 
 * trainDates: a list of dates used to generate training data/features 
 * 
 * dataSplit: data splitting information 
 * 				dataSplit(RecJob.DataSplitting_trainRatio):Double  double (0,1)
 *     			dataSplit(RecJob.DataSplitting_testRatio):Double   double (0,1)
 *     			dataSplit(RecJob.DataSplitting_validRatio):Double  double (0,1)
 * 
 * jobStatus: a data structure maintaining resources for intermediate results. 
 * 
 * sc:         an instance of SparkContext created according to user specification.
 * fs: 		   the file system associated with sc, which can be used to operate HDFS/local FS.  
 */
case class RecJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
	//initialization 
    val jobType = JobType.Recommendation
    
    Logger.logger.info("Parsing job ["+ jobName + "]")
    Logger.logger.info("        job desc:"+ jobDesc)
    val sc:SparkContext = Pipeline.instance.get.sc
    val fs:FileSystem   = Pipeline.instance.get.fs
    val overwriteResource = false
    def outputResource(resourceLoc:String) = Pipeline.outputResource(resourceLoc, overwriteResource)
    
    val resourceLoc:HashMap[String, String] = populateResourceLoc() 
    val featureList:Array[RecJobFeature] = populateFeatures()
    val modelList:Array[RecJobModel] = populateMethods()
    val trainDates:Array[String] = populateTrainDates()
    val testDates:Array[String] = populateTestDates()
    val testList:Array[RecJobTest] = populateTests()
    val metricList:Array[RecJobMetric] = populateMetric()
    
    val dataSplit:HashMap[String, Double] = populateDataSplitting()
    //TODO: parse and ensemble 
    
    val jobStatus:RecJobStatus = new RecJobStatus(this)
    
    Logger.logger.info("Job Parse done => " + this.toString)
    
    /*
     * The main workflow of a recommender system job. 
     */
    def run():Unit= {
    	val logger = Logger.logger 

    	//Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	logger.info("**preparing processing data")
    	DataProcess.prepareTrain(this)
    	
    	logger.info("**preparing features")
    	
    	//preparing features
    	//   for each feature, we generate the resource  
    	this.featureList.foreach{
    		featureUnit =>{
    		    logger.info("*preparing features" + featureUnit.toString())
    		    featureUnit.run(this)
    		    //status: update Job status
    		}
    	}
    	    	
    	//learning models
    	if (this.modelList.length > 0){
    		  
    		logger.info("**learning models")
	    	this.modelList.foreach{
	    	     modelUnit => {
	    	         logger.info("*buildling model" + modelUnit.toString())
	    	         modelUnit.run(this)
	    	     }
	    	}
    	}
    	
    	//testing recommendation performance on testing dates.
    	DataProcess.prepareTest(this)
    	performTestNEvaluation()
    }
    
    
    
    /*
     * perform evaluation of models on test data 
     */
    def performTestNEvaluation() {
    	
    	jobStatus.testWatchTime foreach { testData =>
    		
    		//size of test data
    		Logger.info("Size of test data: " + testData.count)
    		
            //evaluate models on test data
            //go through all regression models
            jobStatus.resourceLocation_RegressModel.keys foreach { k =>
                evaluateModel(jobStatus.resourceLocation_RegressModel(k))
            }//for all regression model
            
            //TODO
            //go through all classification models
            jobStatus.resourceLocation_ClassifyModel.keys foreach { k =>
                evaluateModel(jobStatus.resourceLocation_RegressModel(k))
            }//for all classification model
        }
    }
    
    
    
    /*
     * perform all the test on the passed model and generate score metrics
     */
    def evaluateModel(model: ModelStruct) {
    	//apply all the tests on passed model
        for (test <- testList) {
            
            //prediction results after applying test
            val pred = test.run(this, model)
            
            //perform evaluation if we got predictions above
            pred foreach { predValue =>  
                //perform evaluation on predicted value
                metricList.map {metric => 
                	metric match {
                      case metricSE:RecJobMetricSE => {
                               val score = metricSE.run(predValue.map{x => (x._3, x._4)})
                               Logger.info(s"Evaluated $model $test $metric = $score")
                            }
                      case _ => Logger.warn(s"$metric not known metric")
                    }
                }
           }
        } //end all test
    }
    
    
    
    def generateXML():Option[Elem] = {
       None
    }
    
    /*
     * Create an instance of SparkContext
     * according to specification. 
     * 
     * NOTE: this method is deprecated. Using Pipeline.instance.get.sc to 
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
           Logger.logger.warn("SparkContext specification not found, will try using local.")
       }
          
       if( nodeList.size > 0 && (nodeList(0) \JobTag.RecJobSparkContextJobName).size > 0){
    	   sparkContext_jobName = (nodeList(0) \JobTag.RecJobSparkContextJobName).text
       }  
   
       try{
          return Some(new SparkContext(sparkContext_master, sparkContext_jobName))
       }catch{
         case _:Throwable => Logger.logger.error("Failed to build SparkContext!") 
       }
       
       None
    }
    
    /*
     * Populate data splitting information. 
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
          Logger.logger.warn("Data splitting is not specified for job [%s]".format(jobName))
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
    
    /*
     * Populate resource locations
     */
    def populateResourceLoc():HashMap[String, String] = {
       var resourceLoc:HashMap[String, String] = new HashMap()
       
       var nodeList = jobNode \ JobTag.RecJobResourceLocation
       if (nodeList.size == 0){
          Logger.logger.error("Resource locations are not given. ")
          return resourceLoc
       }
       
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).size > 0) 
    	   resourceLoc(RecJob.ResourceLoc_RoviHQ)     = (nodeList(0) \ JobTag.RecJobResourceLocationRoviHQ).text
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).size > 0) 
    	   resourceLoc(RecJob.ResourceLoc_WatchTime)  = (nodeList(0) \ JobTag.RecJobResourceLocationWatchTime).text
       
       if ((nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).size > 0){ 
	       resourceLoc(RecJob.ResourceLoc_Workspace)  = (nodeList(0) \ JobTag.RecJobResourceLocationWorkspace).text
	       //derivative
	       resourceLoc(RecJob.ResourceLoc_JobData)    = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/data"
	       resourceLoc(RecJob.ResourceLoc_JobFeature) = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/feature"
	       resourceLoc(RecJob.ResourceLoc_JobModel)   = resourceLoc(RecJob.ResourceLoc_Workspace) + "/" +  jobName + "/model"
       }
       
       Logger.logger.info("Resource WATCHTIME:   " + resourceLoc(RecJob.ResourceLoc_WatchTime))
       Logger.logger.info("Resource ROVI:        " + resourceLoc(RecJob.ResourceLoc_RoviHQ))
       Logger.logger.info("Resource Job Data:    " + resourceLoc(RecJob.ResourceLoc_JobData))
       Logger.logger.info("Resource Job Feature: " + resourceLoc(RecJob.ResourceLoc_JobFeature))
       Logger.logger.info("Resource Job Model:   " + resourceLoc(RecJob.ResourceLoc_JobModel))
       resourceLoc
    } 
    
    
    /*
     * Populate training dates. 
     */
    def populateTrainDates():Array[String] = {
      
      var dateList:Seq[String] = Seq()
     
      var nodeList = jobNode \ JobTag.RecJobTrainDateList
      if (nodeList.size == 0){
        Logger.logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTrainDateUnit).map(_.text)
      
      Logger.logger.info("Training dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
      return dateList.toArray
    }
    
    
    /*
     * Populate test dates. 
     */
    def populateTestDates():Array[String] = {
      
      var dateList:Seq[String] = Seq()
     
      var nodeList = jobNode \ JobTag.RecJobTestDateList
      if (nodeList.size == 0){
        Logger.logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RecJobTestDateUnit).map(_.text)
      
      Logger.logger.info("Testing dates: " + dateList.toArray.deep.toString 
          + " hash("+ HashString.generateHash(dateList.toArray.deep.toString) +")")
      return dateList.toArray
    }
    
    
    /*
     * Populate features from XML. 
     */
    def populateFeatures():Array[RecJobFeature] = {
      
      var featList:Array[RecJobFeature] = Array()  
      
      var nodeList = jobNode \ JobTag.RecJobFeatureList 
      if (nodeList.size == 0){
        Logger.logger.warn("No features found!")
        return featList
      } 
      
      nodeList = nodeList(0) \ JobTag.RecJobFeatureUnit 
      
      //populate each feature
      for (node <- nodeList){
        // extract feature type
        val featureType = (node \ JobTag.RecJobFeatureUnitType).text
        
        // extract feature name 
        val featureName = (node \ JobTag.RecJobFeatureUnitName).text
        
        // extract features 
        val featureParam = node \ JobTag.RecJobFeatureUnitParam
        
        var paramList:HashMap[String, String] = HashMap()
        
        for (featureParamNode <- featureParam){
          //in case multiple parameter fields exist. 
          
          // the #PCDATA is currently ignored. 
          val paraPairList = featureParamNode.child.map(feat => (feat.label, feat.text )).filter(_._1 != "#PCDATA")
          
          for (paraPair <- paraPairList){
            paramList += (paraPair._1 -> paraPair._2)
          }
        } 
        
        //create feature structs by type
        featureType match{
          case JobTag.RecJobFeatureType_Item => featList = featList :+ RecJobItemFeature(featureName, paramList)
          case JobTag.RecJobFeatureType_User => featList = featList :+ RecJobUserFeature(featureName, paramList)
          case JobTag.RecJobFeatureType_Fact => featList = featList :+ RecJobFactFeature(featureName, paramList)
          case _ => Logger.logger.warn("Feature type %s not found and discarded.".format(featureType))
        }
        
        Logger.logger.info("Feature found "+ featureType+ ":"+ featureName + ":" + paramList)
      }
      
      featList
    }
    
    
    /*
     * populate required evaluation metrics from XML
     */
    def populateMetric():Array[RecJobMetric] = {
    	var metricList:Array[RecJobMetric] = Array()
    	var nodeList = jobNode \ JobTag.RecJobMetricList
    	if (nodeList.size == 0) {
            Logger.logger.warn("No metrics found!")
            metricList
        } else {
        	nodeList = nodeList(0) \ JobTag.RecJobMetricUnit
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
                                            .filter(_._1 != "#PCDATA") //TODO: why ?
                    for (paraPair <- paraPairList) {
                        paramList += (paraPair._1 -> paraPair._2)
                    }                      
                }
        	    
        	    //create metrics by type
        	    metricType match {
        	    	case JobTag.RecJobMetricType_MSE => metricList = metricList :+ RecJobMetricMSE(metricName, paramList)
        	    	case JobTag.RecJobMetricType_RMSE => metricList = metricList :+ RecJobMetricRMSE(metricName, paramList)
        	    	case _ => Logger.logger.warn("Metric type $metricType not found or ignored.")
        	    }
        	}
        }
    	
    	metricList
    }
    
    
    /*
     * populate test type from XML
     */
    def populateTests():Array[RecJobTest] = {
    	var testList:Array[RecJobTest] = Array()
    	var nodeList = jobNode \ JobTag.RecJobTestList
    	if (nodeList.size == 0){
    	    Logger.logger.warn("No tests found!")
    	    testList
    	} else {
    		nodeList = nodeList(0) \ JobTag.RecJobTestUnit
    		
    		//populate each test
    		for (node <- nodeList) {
    			val testType = (node \ JobTag.RecJobTestUnitType).text
    			val testName = (node \ JobTag.RecJobTestUnitName).text
    			val testParam = node \ JobTag.RecJobTestUnitParam
    			var paramList:HashMap[String, String] = HashMap()
    			
    			//populate test parameters
    			for (param <- testParam) {
    			    val paraPairList = param.child
    			                            .map(line => (line.label, line.text))
    			                            .filter(_._1 != "#PCDATA") //TODO: why ?
    			    for (paraPair <- paraPairList) {
    			    	paramList += (paraPair._1 -> paraPair._2)
    			    }
    			}
    			
    			//create tests by type
    			testType match {
    				case JobTag.RecJobTestType_NotCold => testList = testList :+ RecJobTestNoCold(testName, paramList)
    				case _ => Logger.logger.warn("Test type $testType not found or ignored.")
    			}
    			
    			
    		}
    	}
    	
    	testList
    }
    
    
    /*
     * Populate learning methods from XML. 
     */
    def populateMethods():Array[RecJobModel] = {
      var modelList:Array[RecJobModel] = Array ()
      
      var nodeList = jobNode \ JobTag.RecJobModelList
      if (nodeList.size == 0){
        Logger.logger.warn("No models found!")
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
           case _ => Logger.logger.warn("Model type $modelType not found and ignored.")
         }
      }
      
      //TODO: if there are multiple models, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       s"Job [Recommendation][${this.jobName}][${this.trainDates.length} dates][${this.featureList.length} features][${this.modelList.length} models]"
    }
    
    def getStatus():JobStatus = {
       return this.jobStatus
    }
    
}





/*
 * The recommendation job feature data structure. 
 * 
 * featureName: feature name, used to invoke different feature extraction algorithm 
 * featureParm: feature extraction parameters. 
 * 
 * e.g. RecJobUserFeature("Zapping", (freq -> 10)) 
 * 		RecJobFactFeature("PMF", (k -> 10, pass -> 1))
 */
sealed trait RecJobFeature{
	def run(jobInfo: RecJob):Unit
}

/*
 * Item feature (program feature) e.g., genre 
 */
case class RecJobItemFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	   jobInfo.jobStatus.completedItemFeatures(this) = ItemFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}

/*
 * User feature e.g., watch time, zapping
 */
case class RecJobUserFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	   jobInfo.jobStatus.completedUserFeatures(this) = UserFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}

/*
 * Factorization-based (collaboration filtering) features. 
 */
case class RecJobFactFeature(featureName:String, featureParams:HashMap[String, String]) extends RecJobFeature{
	def run(jobInfo: RecJob) = {
	    jobInfo.jobStatus.completedFactFeatures(this) = FactFeatureHandler.processFeature(featureName, featureParams, jobInfo)
	}
}


/*
 * metric types
 */
sealed trait RecJobMetric 

trait RecJobMetricSE extends RecJobMetric {
	def run(labelNPred: RDD[(Double, Double)]): Double
}

case class RecJobMetricMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
	def run(labelNPred: RDD[(Double, Double)]): Double = {
		//NOTE: can be further extended to use metricName and metricParams like test and model
		ContinuousPrediction.computeMSE(labelNPred)
	}
}


case class RecJobMetricRMSE(metricName: String, metricParams: HashMap[String, String])
    extends RecJobMetricSE {
    def run(labelNPred: RDD[(Double, Double)]): Double = {
        //NOTE: can be further extended to use metricName and metricParams like test and model
        ContinuousPrediction.computeRMSE(labelNPred)
    }
}


/*
 * test types
 */
sealed trait RecJobTest {
	/*
     * run model on test data and return RDD of (user, item, actual label, predicted label)
     */
	def run(jobInfo: RecJob, model:ModelStruct): Option[RDD[(String, String, Double, Double)]]
	
	
}


//Non-coldstart
case class RecJobTestNoCold(testName: String, testParams: HashMap[String, String]) 
    extends RecJobTest {
	/*
	 * run model on test data and return RDD of (user, item, actual label, predicted label)
	 */
	def run(jobInfo: RecJob, model:ModelStruct): Option[RDD[(String, String, Double, Double)]] = {
		
		//TODO: add more models
		model match {
			//get predicted labels
			case linearModel:LinearRegressionModelStruct => 
				Some(LinearRegNotColdTestHandler.performTest(jobInfo, testName, 
						                                testParams, linearModel))
			case _ => None
		}
	}
}


/*
 *  The learning to rank models
 *  
 *  modelName:  model name
 *  modelParam: model parameters
 */
sealed trait RecJobModel{
	def run(jobInfo: RecJob):Unit
}

object RecJobModel{
	val defaultMinUserFeatureCoverage = 0.3
	val defaultMinItemFeatureCoverage = 0.5
  
	val Param_MinUserFeatureCoverage = "minUFCoverage"
	val Param_MinItemFeatureCoverage = "minIFCoverage"
}



/*
 * Regression model
 */
case class RecJobScoreRegModel(modelName:String, modelParams:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with continuous labels (X, y). 
		Logger.logger.info("**assembling data")
    	
		var minIFCoverage = RecJobModel.defaultMinItemFeatureCoverage
		var minUFCoverage = RecJobModel.defaultMinUserFeatureCoverage
		
		//TODO: parse from XML
		
		val dataResourceStr = DataAssemble.assembleContinuousData(jobInfo, minIFCoverage, minUFCoverage)
		
		//2. divide training, testing, validation
		Logger.logger.info("**divide training/testing/validation")
		DataSplitting.splitContinuousData(jobInfo, dataResourceStr, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio)
		)
		
	    //3. train model on training and tune using validation, and testing.
		Logger.logger.info("**building and testing models")
		
		jobInfo.jobStatus.completedRegressModels(this) = 
		    RegressionModelHandler.buildModel(modelName, modelParams, dataResourceStr, jobInfo) 
	}
}



/*
 * Classification model
 */
case class RecJobBinClassModel(modelName:String, modelParams:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {
	    //1. prepare data with binary labels (X, y)
	   Logger.logger.info("**assembling binary data")  
	   
	   var minIFCoverage = RecJobModel.defaultMinItemFeatureCoverage
	   var minUFCoverage = RecJobModel.defaultMinUserFeatureCoverage
	   
	   var balanceTraining = false
	   
	   //TODO: parse from XML
	   
	   val dataResourceStr = DataAssemble.assembleBinaryData(jobInfo, minIFCoverage, minUFCoverage)
	   
	   //2. divide training, testing, validation
	   Logger.logger.info("**divide training/testing/validation")
	   DataSplitting.splitBinaryData(jobInfo, dataResourceStr, 
		    jobInfo.dataSplit(RecJob.DataSplitting_trainRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_testRatio),
		    jobInfo.dataSplit(RecJob.DataSplitting_validRatio),
		    balanceTraining
		)
	   
	   //3. train model on training and tune using validation, and testing.
	   Logger.logger.info("**building and testing models")
	   
	   jobInfo.jobStatus.completedClassifyModels(this) = 
	       ClassificationModelHandler.buildModel(modelName, modelParams, dataResourceStr, jobInfo)
       
	}
}




