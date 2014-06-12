package com.samsung.vddil.recsys.feature.fact

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource

/*
 * Factorization Feature: Probabilistic Matrix Factorization
 */
object FactFeaturePMF  extends FeatureProcessingUnit{
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		new FeatureResource(false, null)
	}
}