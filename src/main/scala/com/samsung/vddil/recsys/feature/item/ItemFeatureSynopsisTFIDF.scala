package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger

/*
 * Item Feature: extract TFIDF numerical features from synopsis
 */
object ItemFeatureSynopsisTFIDF extends FeatureProcessingUnit {
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
		FeatureResource.fail
	}
	
	val IdenPrefix:String = "ItemFeatureSynTFIDF"
   
}