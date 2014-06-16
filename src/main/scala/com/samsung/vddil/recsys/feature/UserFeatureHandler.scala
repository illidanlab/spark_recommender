package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.user.UserFeatureBehaviorWatchtime
import com.samsung.vddil.recsys.feature.user.UserFeatureBehaviorZapping
import com.samsung.vddil.recsys.feature.user.UserFeatureBehaviorGenre

/*
 * This is the main entrance of the user feature processing.
 * 
 * TODO: change this component to dynamic class loading. 
 */
object UserFeatureHandler extends FeatureHandler{
	val UFBehaviorWatchtime = "watchtime"
	val UFBehaviorZapping   = "zap"
	val UFBehaviorGenre     = "genre"
	
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob):Boolean={
		Logger.logger.info("Processing user feature [%s:%s]".format(featureName, featureParams))
	
		var resource:FeatureResource = FeatureResource.fail
	    
		//Process the features accordingly.
		featureName match{
	      case UFBehaviorWatchtime => resource = UserFeatureBehaviorWatchtime.processFeature(featureParams, jobInfo)
	      case UFBehaviorZapping   => resource = UserFeatureBehaviorZapping.processFeature(featureParams, jobInfo)
	      case UFBehaviorGenre     => resource = UserFeatureBehaviorGenre.processFeature(featureParams, jobInfo)
	      case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
	    }
	    
		//For the successful ones, push resource information to jobInfo.jobStatus. 
	    if(resource.success){
		   resource.resourceMap(FeatureResource.ResourceStr_UserFeature) match{
		      case resourceStr:String => 
		        jobInfo.jobStatus.resourceLocation_UserFeature(resource.resourceIden) = resourceStr
		   }
		   
		   resource.resourceMap(FeatureResource.ResourceStr_UserFeatureMap) match{
		     case resourceStr:String =>
		        jobInfo.jobStatus.resourceLocation_UserFeatureMap(resource.resourceIden) = resourceStr
		   }
		}
	    
	    resource.success
		  
	}
}