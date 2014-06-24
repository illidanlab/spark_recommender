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
object ItemFeatureHandler extends FeatureHandler{
	//predefined values for feature name 
	val IFSynopsisTopic:String = "syn_topic"
	val IFSynopsisTFIDF:String = "syn_tfidf"
	val IFGenre:String = "genre"
  
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean = {
		Logger.logger.info("Processing item feature [%s:%s]".format(featureName, featureParams))
		 
		var resource:FeatureResource = FeatureResource.fail
		
		//Process the features accordingly
		featureName match{
		  case IFSynopsisTopic => resource = ItemFeatureSynopsisTopic.processFeature(featureParams, jobInfo)
		  case IFSynopsisTFIDF => resource = ItemFeatureSynopsisTFIDF.processFeature(featureParams, jobInfo)
		  case IFGenre =>         resource = ItemFeatureGenre.processFeature(featureParams, jobInfo)
		  case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
		}
		
		//For the successful ones, push resource information to jobInfo.jobStatus.
		if(resource.success){
		   resource.resourceMap.get(FeatureResource.ResourceStr_ItemFeature) match{
		      case featureStruct:ItemFeatureStruct=> 
		        jobInfo.jobStatus.resourceLocation_ItemFeature(resource.resourceIden) = featureStruct
		   }
		}
		
		resource.success
	}
}