package com.samsung.vddil.recsys.feature

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess

/**
 * This is a trait for feature processing unit. 
 * 
 * Each feature processing unit is an object that 
 * provides a static method process feature  
 *  
 *  @author jiayu.zhou
 */
trait FeatureProcessingUnit {
	/*
	 * Each feature process method takes the feature parameters and job info, process the feature
	 * and then store access information (the location of the feature in HDFS), and results (success/fail) 
	 * in the FeatureResource data structure.  
	 * 
	 * General steps. 
	 * 1. Complete default parameters 
	 * 2. Generate resource identity using resouceIdentity()
	 * 3. Feature generation algorithms (HDFS operations)
	 * 4. Generate and return a FeatureResource that includes all resources.  
	 * 
	 */
	def processFeature(
	        featureParams:HashMap[String, String],
	        postProcessing:List[FeaturePostProcess],  
	        jobInfo:RecJob):FeatureResource
	
	/*
	 * Given a feature parameter map, this method provides a string that uniquely determines this feature.
	 * Note that this resource identity should include all parameters in order to generate a string. Therefore
	 * default values not specified in job should be completed in the featureParam before sending to resource identity.  
	 */
	def IdenPrefix: String
	def resourceIdentity(featureParam:HashMap[String, String], dataIdentifier:String):String = {
        IdenPrefix + "_" + HashString.generateHash(featureParam.toString) + "_" + dataIdentifier 
    }

    def checkIdentity(ideString:String):Boolean = {
        ideString.startsWith(IdenPrefix)
    }
	
}
