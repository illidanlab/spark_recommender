package com.samsung.vddil.recsys.prediction

import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.testing._
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import scala.collection.mutable.HashMap
import org.apache.spark.SparkContext

/**
 * Defines how the prediction should be done. 
 */
case class RecJobPrediction (
        val dates:Array[String],
        val contentDates:Array[String],
        val params:HashMap[String, String]
	) {
    
    
    val IdenPrefix = "Prediction"
        
    val paramTopKDefault = "5"
    val paramTopKField   = "topK"
        
    /**
     *  
     */   
    def run(jobInfo:RecJob, model:ModelStruct){
        
        val topK = params.getOrElseUpdate(paramTopKField, paramTopKDefault).toInt
        
        dates.foreach{ predDate =>
            generateRecResult(jobInfo, model, contentDates.toList, predDate, topK)
        }
    }
     
    /**
     * @param jobInfo
     * @param model a ModelStruct. 
     * @param knowledgeDate this is the date where content features will be extracted.  
     * @param predictDate this is the date where the prediction is going to be done.
     * @param the topK prediction. 
     */
    private def generateRecResult(
            jobInfo:RecJob, 
	        model:ModelStruct, 
	        contentDates:List[String],
	        predictDate:String,
	        topK:Int) = {
        
        val predDir            = jobInfo.resourceLoc(RecJob.ResourceLoc_Workspace) + s"/Prediction/Result/$predictDate"
        val predResourceDir    = jobInfo.resourceLoc(RecJob.ResourceLoc_Workspace) + s"/Prediction/Resource/$predictDate"
        val contentDate:String = contentDates(0)
        
        //TODO: extract from ROVI schedule. yuan's module
        val roviPath     = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + "/" + contentDate + "/schedule.txt.gz"
        val roviSchedule = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + "/" + contentDate + "/schedule.txt.gz"
        
        val itemList:Set[String] = 
            RecJobPrediction.getProgramList(jobInfo.sc, predictDate, roviSchedule).collect().toSet
        val itemListSize = itemList.size
            
        Logger.info(s"In $contentDate program schedule, there are in total $itemListSize programs on date $predictDate")
            
        //generate prediction scores for all users and specified items. 
        val prediction = 
            generatePredictionScores(jobInfo, model, contentDates, predResourceDir, itemList)
        Logger.info("Total (uid, pid, score) entries are " + prediction.count())
        
        //perform recommendation. 
        RecOutput.doRecommendation(jobInfo.sc, predictDate, roviPath, prediction, predDir, topK)
    }
    
    /**
     * 
     */
	private def generatePredictionScores(
	        jobInfo:RecJob, 
	        model:ModelStruct, 
	        contentDates:List[String],
	        predResourceDir:String,
	        itemList:Set[String]): 
		RDD[(Int, (String, Double))] ={
	    
        val sc = jobInfo.sc
        
	    val itemFeatObjFile      = predResourceDir + "/" + IdenPrefix + "/itemFeat"   
	    val userFeatObjFile      = predResourceDir + "/" + IdenPrefix + "/userFeat" 
	    val itemUserFeatFile     = predResourceDir + "/" + IdenPrefix + "/sampledUserItemFeat"
	    val predBlockFiles       = predResourceDir + "/" + IdenPrefix + "/sampledPred/BlockFiles"
	    val partialModelBatchNum = 10
	    val partitionNum = jobInfo.partitionNum_test
	    
	    //get user features (all users in training)
	    Logger.info("Preparing user features...")
	    
	    val userFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                        .userFeatureOrder
                                        
        val allUsers = jobInfo.jobStatus.resourceLocation_CombinedData_train.get.getUserMap().map{x=>x._2}
        
	    if (jobInfo.outputResource(userFeatObjFile)){
	      val userFeatures:RDD[(Int, Vector)] = getOrderedFeatures(allUsers, userFeatureOrder, sc)
	      userFeatures.saveAsObjectFile(userFeatObjFile)
	    }
	    val userFeatures:RDD[(Int, Vector)] = sc.objectFile[(Int, Vector)](userFeatObjFile)
	    Logger.info("No. of users: " + userFeatures.count)
	    
	    
	    //get item features
	    Logger.info("Preparing item features..")
	    val itemFeatureOrder = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(model.learnDataResourceStr)
                                 .itemFeatureOrder.map{feature => feature.asInstanceOf[ItemFeatureStruct]}
      
		if (jobInfo.outputResource(itemFeatObjFile)) {
		  val coldItemFeatures:RDD[(String, Vector)] = getColdItemFeatures(itemList,
		    jobInfo, itemFeatureOrder, contentDates)
		  coldItemFeatures.saveAsObjectFile(itemFeatObjFile)
		}
		val coldItemFeatures:RDD[(String, Vector)] = sc.objectFile[(String, Vector)](itemFeatObjFile)
		Logger.info("No. of users: " + userFeatures.count)
		
		//use predictions to get recall and hits
		val userItemPred:RDD[(Int, (String, Double))] = computePrediction (
            	model,  userFeatures, coldItemFeatures,
            	(resLoc: String) => jobInfo.outputResource(resLoc),
            	predBlockFiles, itemUserFeatFile,
            	sc, partitionNum, partialModelBatchNum
    		)    
		
		userItemPred
	}
}

object RecJobPrediction{
     /**
     * NOTE: this function depends on the schema of ROVI fields. 
     */
    def getProgramList(paraSc: SparkContext, date: String, roviPath: String): RDD[String] = {
    	val roviProgramList = paraSc.textFile(roviPath).
    			map(_.split("""\|""")).
    			filter(data => data(2).substring(0, 8) == date).
    			map(data => data(4)).distinct
        return roviProgramList
    }
}
