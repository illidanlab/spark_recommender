package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTFIDF
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTopic
import com.samsung.vddil.recsys.feature.item.ItemFeatureGenre

/*
 * This is the main entrance of the item (program) feature processing.
 * 
 * TODO: change this component to dynamic class loading.
 */
object ItemFeatureHandler {
	//predefined values for feature name 
	val IFSynopsisTopic:String = "syn_topic"
	val IFSynopsisTFIDF:String = "syn_tfidf"
	val IFGenre:String = "genre"
  
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.info("Processing item feature ["+ featureName + ":" + featureParams + "]")
		Logger.logger.info("Processing item feature [%s : %s]".format(featureName, featureParams))
		 
		//Process the features accordingly 
		featureName match{
		  case IFSynopsisTopic => ItemFeatureSynopsisTopic.processFeature(featureParams, jobInfo)
		  case IFSynopsisTFIDF => ItemFeatureSynopsisTFIDF.processFeature(featureParams, jobInfo)
		  case IFGenre =>         ItemFeatureGenre.processFeature(featureParams, jobInfo)
		  case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
		}
		
		//TODO: After features is done, add appropriate information in the jobInfo.status
	}
}