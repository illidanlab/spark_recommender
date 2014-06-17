package com.samsung.vddil.recsys.feature.fact

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString

/*
 * Factorization Feature: Probabilistic Matrix Factorization
 */
object FactFeaturePMF  extends FeatureProcessingUnit{
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
		FeatureResource.fail
	}
	
	val IdenPrefix:String = "FactFeaturePMF"
    
    def resourceIdentity(featureParam:HashMap[String, String]):String = {
        IdenPrefix + "_" + HashString.generateHash(featureParam.toString)
    }
    
    def checkIdentity(ideString:String):Boolean = {
        ideString.startsWith(IdenPrefix)
    }
	
}