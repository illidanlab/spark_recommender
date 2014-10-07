package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.feature.item.ItemFeatureExtractor
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTFIDF
import com.samsung.vddil.recsys.feature.item.ItemFeatureSynopsisTopic
import com.samsung.vddil.recsys.feature.item.ItemFeatureGenre
import com.samsung.vddil.recsys.utils.Logger
import scala.collection.mutable.{Map=>MMap}
import com.samsung.vddil.recsys.feature.item.ItemFeatureShowTime
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
	val IFShowTime:String = "showTime"

    //this will contain reverse mapping from resource string to Feature object 
    //val revItemFeatureMap:MMap[String, ItemFeatureExtractor] = MMap.empty

	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean = {
		Logger.logger.info("Processing item feature [%s:%s]".format(featureName, featureParams))
		 
		var resource:FeatureResource = FeatureResource.fail
		
		//Process the features accordingly
		featureName match{
		  case IFSynopsisTopic => resource = ItemFeatureSynopsisTopic.processFeature(featureParams, jobInfo)
		  case IFSynopsisTFIDF => resource = ItemFeatureSynopsisTFIDF.processFeature(featureParams, jobInfo)
		  case IFGenre         => resource = ItemFeatureGenre.processFeature(featureParams, jobInfo)
		  case IFShowTime      => resource = ItemFeatureShowTime.processFeature(featureParams, jobInfo)
		  case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
		}
		
		//For the successful ones, push resource information to jobInfo.jobStatus.
		if(resource.success){
		   resource.resourceMap.get(FeatureResource.ResourceStr_ItemFeature) match{
		      case featureStruct:ItemFeatureStruct=> 
		        jobInfo.jobStatus.resourceLocation_ItemFeature(resource.resourceIden) = featureStruct
		   }

//       featureName match{
//		    case IFSynopsisTFIDF => revItemFeatureMap(resource.resourceIden) = ItemFeatureSynopsisTFIDF
//		    case IFGenre =>         revItemFeatureMap(resource.resourceIden) = ItemFeatureGenre
//		    case _ => Logger.logger.warn("Unknown item feature type for reverse feature map [%s]".format(featureName))
//		  }

		}
		
		resource.success
	}


}
