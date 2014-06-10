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
    
    Logger.logger.info("Job Parse => " + this.toString)
    
    
    
    /*
     * Populate training dates. 
     */
    def populateTrainDates():Array[String] = {
      
      var dateList:Seq[String] = Seq()
     
      var nodeList = jobNode \ JobTag.RECJOB_TRAIN_DATE_LIST
      if (nodeList.size == 0){
        Logger.logger.warn("No training dates given!")
        return dateList.toArray
      }
      
      dateList = (nodeList(0) \ JobTag.RECJOB_TRAIN_DATE_UNIT).map(_.text)
      
      return dateList.toArray
    }
    
    /*
     * Populate features from XML. 
     */
    def populateFeatures():Array[RecJobFeature] = {
      
      var featList:Array[RecJobFeature] = Array()  
      
      var nodeList = jobNode \ JobTag.RECJOB_FEATURE_LIST 
      if (nodeList.size == 0){
        Logger.logger.warn("No features found!")
        return featList
      } 
      
      nodeList = nodeList(0) \ JobTag.RECJOB_FEATURE_UNIT 
      
      //populate each feature
      for (node <- nodeList){
        // extract feature type
        val featureType = (node \ JobTag.RECJOB_FEATURE_UNIT_TYPE).text
        
        // extract feature name 
        val featureName = (node \ JobTag.RECJOB_FEATURE_UNIT_NAME).text
        
        // extract features 
        val featureParam = node \ JobTag.RECJOB_FEATURE_UNIT_PARAM
        
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
          case JobTag.RECJOB_FEATURE_TYPE_ITEM => featList = featList :+ RecJobItemFeature(featureName, paramList)
          case JobTag.RECJOB_FEATURE_TYPE_USER => featList = featList :+ RecJobUserFeature(featureName, paramList)
          case JobTag.RECJOB_FEATURE_TYPE_FACT => featList = featList :+ RecJobFactFeature(featureName, paramList)
          case _ => Logger.logger.warn("Feature type "+ featureType+ " not found and discarded. ")
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
      
      var nodeList = jobNode \ JobTag.RECJOB_MODEL_LIST
      if (nodeList.size == 0){
        Logger.logger.warn("No models found!")
        return modelList
      }
      
      nodeList = nodeList(0) \ JobTag.RECJOB_MODEL_UNIT
      
      //populate each model. 
      for (node <- nodeList){
         val modelType = (node \ JobTag.RECJOB_MODEL_UNIT_TYPE).text
         
         val modelName = (node \ JobTag.RECJOB_MODEL_UNIT_NAME).text
         
         val modelParam = node \ JobTag.RECJOB_MODEL_UNIT_PARAM
         
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
           case JobTag.RECJOB_MODEL_TYPE_SCRREG => modelList = modelList :+ RecJobScoreRegModel(modelName, paramList)
           case JobTag.RECJOB_MODEL_TYPE_BINCLS => modelList = modelList :+ RecJobBinClassModel(modelName, paramList)
           case _ => Logger.logger.warn("Model type "+ modelType+ " not found and ignored.")
         }
      }
      
      //TODO: if there are multiple model, then we need to also specify an ensemble type. 
      
      modelList
    }
    
    override def toString():String = {
       "Job [Recommendation][" + this.jobName + "]["+ this.trainDates.length +" dates][" + this.featureList.length + " features][" + this.modelList.length + " models]"
    }
    
    def run():Unit= {
    	val logger = Logger.logger 
        
    	//TODO: consider cache each of these components. 
    	//      the cache may be done in file system level. 
    	
    	//preparing processing data. 
    	logger.info("**preparing processing data")
    	
    	
    	logger.info("**preparing features")
    	//preparing features
    	this.featureList.foreach{
    		featureUnit =>{
    		    logger.info("*preparing features" + featureUnit.toString())
    		}
    	}
    	    	
    	//assemble training/validation/testing cases for training data. 
    	
    	
    	//learning models
    	
    	logger.info("**learning models")
    	this.modelList.foreach{
    	   modelUnit => {
    	     logger.info("*buildling model" + modelUnit.toString())
    	   }
    	}
    	
    	//testing recommendation performance. 
    	logger.info("**testing models")
    	
    	
    }
    
    /*
     * featureName: feature name, used to invoke different feature extraction algorithm 
     * featureParm: feature extraction parameters. 
     * 
     * e.g. RecJobUserFeature("Zapping", (freq -> 10)) 
     * 		RecJobFactFeature("PMF", (k -> 10, pass -> 1))
     */
    sealed trait RecJobFeature  
    case class RecJobItemFeature(featureName:String, featureParam:HashMap[String, String]) extends RecJobFeature
    case class RecJobUserFeature(featureName:String, featureParam:HashMap[String, String]) extends RecJobFeature
    case class RecJobFactFeature(featureName:String, featureParam:HashMap[String, String]) extends RecJobFeature
	
	sealed trait RecJobModel
    case class RecJobScoreRegModel(modelName:String, modelParam:HashMap[String, String]) extends RecJobModel
    case class RecJobBinClassModel(modelName:String, modelParam:HashMap[String, String]) extends RecJobModel
}


