/*
 * Recommendation Job
 * 
 * @author jiayu.zhou
 * 
 */

package com.samsung.vddil.recsys.job

import scala.xml._
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.feature.ItemFeatureHandler
import com.samsung.vddil.recsys.data.DataProcess
import scala.collection.mutable.HashSet
import com.samsung.vddil.recsys.feature.UserFeatureHandler
import com.samsung.vddil.recsys.feature.FactFeatureHandler

/*
 * The information about a particular recommendation Job. 
 */
case class RecJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
	
	//initialization 
    val jobType = JobType.Recommendation
    
    
    Logger.logger.info("Parsing job ["+ jobName + "]")
    Logger.logger.info("        job desc:"+ jobDesc)
    val featureList:Array[RecJobFeature] = populateFeatures()
    val modelList:Array[RecJobModel] = populateMethods()
    val trainDates:Array[String] = populateTrainDates()
     
    val jobStatus:RecJobStatus = new RecJobStatus(this)
    
    Logger.logger.info("Job Parse => " + this.toString)
    
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
    
    /*
     * The main workflow of a recommender system job. 
     */
    def run():Unit= {
    	val logger = Logger.logger 
        
    	//TODO: consider cache each of these components. 
    	//      the cache may be done in file system level. 
    	
    	//Preparing processing data. 
    	//In this step the user/item lists are available in the JobStatus. 
    	logger.info("**preparing processing data")
    	DataProcess.prepare(this)
    	
    	logger.info("**preparing features")
    	//preparing features
    	this.featureList.foreach{
    		featureUnit =>{
    		    logger.info("*preparing features" + featureUnit.toString())
    		    featureUnit.run(this)
    		    //status: update Job status
    		}
    	}
    	    	
    	//learning models
    	if (this.modelList.length > 0){
    		logger.info("**assembling data")
    		//TODO: assembling module 
    		
    		logger.info("**divide training/testing/validation")
    		//TODO: split data
    		
    		logger.info("**learning models")
	    	this.modelList.foreach{
	    	     modelUnit => {
	    	         logger.info("*buildling model" + modelUnit.toString())
	    	         modelUnit.run(this)
	    	         //status: update Job status
	    	     }
	    	}
    		
    		//testing recommendation performance. 
    		
    		logger.info("**testing models")
    		//TODO: testing models. 
    	}
    }
    
    def getStatus():JobStatus = {
       return this.jobStatus
    }
    
    
}


case class RecJobStatus(jobInfo:RecJob) extends JobStatus{
	// Use the RecJob to initialize the RecJobStatus
	// so we know what are things that we want to keep track.
	
	/*
	 * Here we store the location of the resources (prepared data, features, models). 
	 */ 
	//val resourceLocation:HashMap[Any, String] = new HashMap() // a general place.
	val resourceLocation_UserFeature:HashMap[String, String] = new HashMap() 
	val resourceLocation_ItemFeature:HashMap[String, String] = new HashMap()
	val resourceLocation_ClassifyModel:HashMap[String, String] = new HashMap()
	val resourceLocation_RegressModel:HashMap[String, String] = new HashMap()
	
	/*
	 *  As set of flags showing completed components. 
	 */
	val completedItemFeatures:HashSet[RecJobItemFeature] = new HashSet()
	val completedUserFeatures:HashSet[RecJobUserFeature] = new HashSet()
	val completedFactFeatures:HashSet[RecJobFactFeature] = new HashSet()
	val completedRegressModels:HashSet[RecJobModel] = new HashSet()
	val completedClassifyModels:HashSet[RecJobModel] = new HashSet()
	
    def allCompleted():Boolean = {
       true
    }
    
    def showStatus():Unit = {
    	Logger.logger.info("Completed Item Features " + completedItemFeatures)
    	Logger.logger.info("Completed User Features " + completedItemFeatures)
    	Logger.logger.info("Completed Fact Features " + completedItemFeatures)
    	Logger.logger.info("Completed Regression Models " + completedItemFeatures)
    	Logger.logger.info("Completed Classification Models " + completedItemFeatures)
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
 *  The learning to rank model 
 */
sealed trait RecJobModel{
	def run(jobInfo: RecJob):Unit
}

/*
 * Regression model
 */
case class RecJobScoreRegModel(modelName:String, modelParam:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {}
}

/*
 * Classification model
 */
case class RecJobBinClassModel(modelName:String, modelParam:HashMap[String, String]) extends RecJobModel{
	def run(jobInfo: RecJob):Unit = {}
}