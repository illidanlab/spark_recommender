package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString

/*
 * User Feature: Zapping features
 */
object UserFeatureBehaviorZapping extends FeatureProcessingUnit {
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob): FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
		FeatureResource.fail
	}
	
	def resourceIdentity(featureParam:HashMap[String, String]):String = {
	    "UserFeatureZapping_" + HashString.generateHash(featureParam.toString)
	}
}