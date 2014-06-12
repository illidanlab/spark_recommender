package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.user.UserFeatureBehaviorWatchtime
import com.samsung.vddil.recsys.feature.user.UserFeatureBehaviorZapping

/*
 * This is the main entrance of the user feature processing.
 * 
 * TODO: change this component to dynamic class loading. 
 */
object UserFeatureHandler {
	val UFBehaviorWatchtime = "watchtime"
	val UFBehaviorZapping   = "zap"
	
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean={
		Logger.logger.info("Processing user feature [%s:%s]".format(featureName, featureParams))
	
		//Process the features accordingly.
		var resource:FeatureResource = new FeatureResource(false)
	    
		featureName match{
	      case UFBehaviorWatchtime => resource = UserFeatureBehaviorWatchtime.processFeature(featureParams, jobInfo)
	      case UFBehaviorZapping   => resource = UserFeatureBehaviorZapping.processFeature(featureParams, jobInfo)
	      case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
	    }
	    
	    if(resource.success){
		   resource.resourceMap(FeatureResource.ResourceStr_UserFeature) match{
		      case resourceStr:String => 
		        jobInfo.jobStatus.resourceLocation_UserFeature("blah") = resourceStr
		   }
		}
	    
	    resource.success
		  
	}
}