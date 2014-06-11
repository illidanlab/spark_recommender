package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTFIDF
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTopic

/*
 * This is the main entrance of the item (program) feature processing.  
 */
object ItemFeatureHandler {
	//predefined values for feature name 
	val ITEM_FEATURE_SYNOPSIS_TOPIC = "syn_topic"
	val ITEM_FEATURE_SYNOPSIS_TFIDR = "syn_tfidf"
  
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.info("Processing item feature ["+ featureName + ":" + featureParams + "]")
		Logger.logger.info("Processing item feature [%s : %s]".format(featureName, featureParams))
		 
		featureName match{
		  case ITEM_FEATURE_SYNOPSIS_TOPIC => ItemFeatureSynopsisTopic.processFeature(featureName, featureParams, jobInfo)
		  case ITEM_FEATURE_SYNOPSIS_TFIDR => ItemFeatureSynopsisTFIDF.processFeature(featureName, featureParams, jobInfo)
		  case _ => Logger.logger.info("Unknown item feature type [%s]".format(featureName))
		}
	}
}