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

case class RecJob (jobName:String, jobDesc:String, jobNode:Node) extends Job {
	
	//initialization 
    val jobType = JobType.Recommendation
    Logger.logger.info("Parsing job ["+ jobName + "]")
    Logger.logger.info("        job desc:"+ jobDesc)
    val featureList = populateFeatures()
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
     * Populate features. 
     */
    def populateFeatures():Array[RecJobFeature] = {
      
      var featList:Array[RecJobFeature] = Array()  
      
      var nodeList = jobNode \ JobTag.RECJOB_FEATURE_LIST 
      if (nodeList.size == 0){
        Logger.logger.warn("No features found!")
        return featList
      } 
      
      nodeList = nodeList(0) \ JobTag.RECJOB_FEATURE_UNIT 
      
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
        
        featureType match{
          case JobTag.RECJOB_FEATURE_TYPE_ITEM => featList = featList :+ RecJobItemFeature(featureName, paramList)
          case JobTag.RECJOB_FEATURE_TYPE_USER => featList = featList :+ RecJobUserFeature(featureName, paramList)
          case JobTag.RECJOB_FEATURE_TYPE_FACT => featList = featList :+ RecJobFactFeature(featureName, paramList)
          case _ => Logger.logger.warn("Feature type "+ featureType+ " not found and discarded. ")
        }
        
        Logger.logger.info("Feature found "+ featureType+ ":"+ featureName + ":" + paramList)
      }
      
      return featList
    }
    
    
    override def toString():String = {
       "Job [Recommendation][" + this.jobName + "]["+ this.trainDates.length +" dates][" + this.featureList.length + " features]"
    }
    
    def run():Unit= {
    	val logger = Logger.logger 
        
    	//TODO: consider cache each of these components. 
    	//      the cache may be 
    	
    	//preparing processing data. 
    	logger.info("**preparing processing data")
    	
    	//preparing features
    	logger.info("**preparing item features")
    	
    	logger.info("**preparing user features")
    	
    	logger.info("**preparing factorization features")
    	
    	//assemble training/validation/testing cases for training data. 
    	
    	
    	//learning models
    	
    	logger.info("**learning models")
    	
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
	
	sealed trait RecJobLearningMethod
}


