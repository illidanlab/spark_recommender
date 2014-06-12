package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.fact.FactFeatureNMF
import com.samsung.vddil.recsys.feature.fact.FactFeaturePMF

/*
 * This is the main entrance of the factorization feature processing.
 * 
 * TODO: change this component to dynamic class loading.
 */
object FactFeatureHandler {
	val FFNMF = "nmf"
	val FFPMF = "pmf"
	  
	def processFeature(featureName:String, featureParams:HashMap[String, String], jobInfo:RecJob) = {
		Logger.logger.info("Processing factorization feature ["+ featureName + ":" + featureParams + "]")
		Logger.logger.info("Processing factorization feature [%s : %s]".format(featureName, featureParams))
		 
		//Process the features accordingly 
		featureName match{
		  case FFNMF => FactFeatureNMF.processFeature(featureParams, jobInfo)
		  case FFPMF => FactFeaturePMF.processFeature(featureParams, jobInfo)
		  case _ => Logger.logger.warn("Unknown item feature type [%s]".format(featureName))
		}
		
		//TODO: After features is done, add appropriate information in the jobInfo.status
		
	}
}