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
	
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob)={
	    Logger.logger.info("Processing user feature ["+ featureName + ":" + featureParams + "]")
		Logger.logger.info("Processing user feature [%s : %s]".format(featureName, featureParams))
	
		//Process the features accordingly.
		featureName match{
	      case UFBehaviorWatchtime => UserFeatureBehaviorWatchtime.processFeature(featureParams, jobInfo)
	      case UFBehaviorZapping   => UserFeatureBehaviorZapping.processFeature(featureParams, jobInfo)
	      case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
	    }
	    
		//TODO: After features is done, add appropriate information in the jobInfo.status	    
	}
}