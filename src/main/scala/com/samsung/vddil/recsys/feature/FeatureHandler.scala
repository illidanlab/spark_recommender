package com.samsung.vddil.recsys.feature

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess

/**
 * The feature handler implementations are used to process a specified type of features.  
 * 
 * @author jiayu.zhou
 */
trait FeatureHandler {
	/*
	 * This method process one type of features, and return a boolean value indicating if the feature 
	 * generation is successful or failed. For the successful ones, this method should push resource 
	 * information to jobInfo.jobStatus. 
	 */
	def processFeature(
	        featureName:String, 
	        featureParams:HashMap[String, String], 
	        postProcessing:List[FeaturePostProcess], 
	        jobInfo:RecJob):Boolean
}