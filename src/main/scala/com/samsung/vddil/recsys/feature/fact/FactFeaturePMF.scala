package com.samsung.vddil.recsys.feature.fact

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit

/*
 * Factorization Feature: Probabilistic Matrix Factorization
 */
object FactFeaturePMF  extends FeatureProcessingUnit{
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
	}
}