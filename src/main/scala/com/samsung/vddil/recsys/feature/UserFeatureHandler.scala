package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.feature.user._
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.Logger
import scala.collection.mutable.HashMap

/*
 * This is the main entrance of the user feature processing.
 * 
 * TODO: change this component to dynamic class loading. 
 */
object UserFeatureHandler extends FeatureHandler{
	val UFBehaviorWatchtime = "watchtime"
	val UFBehaviorZapping   = "zap"
	val UFBehaviorGenre     = "genre"
	val UFBehaviorTFIDF     = "syn_tfidf"

	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean={
		Logger.info("Processing user feature [%s:%s]".format(featureName, featureParams))
	
		var resource:FeatureResource = FeatureResource.fail
	    
		//Process the features accordingly.
		featureName match{
	      case UFBehaviorWatchtime => resource = UserFeatureBehaviorWatchtime.processFeature(featureParams, jobInfo)
	      case UFBehaviorZapping   => resource = UserFeatureBehaviorZapping.processFeature(featureParams, jobInfo)
	      case UFBehaviorGenre     => resource = UserFeatureBehaviorGenre.processFeature(featureParams, jobInfo)
	      case UFBehaviorTFIDF     => resource = UserFeatureBehaviorSynTFIDF.processFeature(featureParams, jobInfo)
	      case _ => Logger.warn("Unknown item feature type [%s]".format(featureName))
	    }
	    
		//For the successful ones, push resource information to jobInfo.jobStatus. 
	    if(resource.success){
		   resource.resourceMap.get(FeatureResource.ResourceStr_UserFeature) match{
		      case featureStruct:UserFeatureStruct => 
		        jobInfo.jobStatus.resourceLocation_UserFeature(resource.resourceIden) = featureStruct
		   }
		}
	    
	    resource.success
		  
	}
}
