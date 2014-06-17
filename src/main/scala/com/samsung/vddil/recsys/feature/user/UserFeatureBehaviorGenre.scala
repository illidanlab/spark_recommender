package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString

object UserFeatureBehaviorGenre extends FeatureProcessingUnit{
	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
		
		//get spark context
		val sc = jobInfo.sc
		
		// 1. Complete default parameters
		
		
	    // 2. Generate resource identity using resouceIdentity()
		var resourceIden = resourceIdentity(featureParams)
		
	    // 3. Feature generation algorithms (HDFS operations)
		
		//get item genres
		
		val mergedData = sc.textFile(jobInfo.jobStatus.resourceLocation_CombineData)
						   .map { line =>
						   			val fields = line.split(',')
						   			//user,item, watchtime
						   			val item = fields(2)
						   			(fields(0), fields(1), fields(2)) 
						 }
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
		FeatureResource.fail
	}
	
	def resourceIdentity(featureParam:HashMap[String, String]):String = {
	    "UserFeatureGenre_" + HashString.generateHash(featureParam.toString)
	}
}