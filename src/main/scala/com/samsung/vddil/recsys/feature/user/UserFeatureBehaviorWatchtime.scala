package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit

/*
 * User Feature: Watch time features. 
 */
object UserFeatureBehaviorWatchtime extends FeatureProcessingUnit {
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
	}
}