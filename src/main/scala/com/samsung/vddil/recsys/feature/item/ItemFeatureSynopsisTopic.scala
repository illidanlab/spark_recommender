package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap

/*
 * Item Feature: extract topic related features from synopsis 
 */
object ItemFeatureSynopsisTopic {
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
	}
}