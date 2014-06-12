package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource

/*
 * Item Feature: extract topic related features from synopsis 
 */
object ItemFeatureSynopsisTopic extends FeatureProcessingUnit {
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		new FeatureResource(false, null)
	}
}