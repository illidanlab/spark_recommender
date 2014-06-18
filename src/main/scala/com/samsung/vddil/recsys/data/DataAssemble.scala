package com.samsung.vddil.recsys.data


import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import com.samsung.vddil.recsys.utils.HashString

object DataAssemble {
	/*
	 *  Joining features 
	 */
	def assembleContinuousData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double ):String = {
		
	  
	    //1. inspect all available features
		//   drop features have low coverage (which significant reduces our training due to missing)
	    //   TODO: minUserFeatureCoverage and minItemFeatureCoverage from file. 
		
	  
		//2. perform an intersection on selected user features, generate <intersectUF>
		var intersectUserFeature:HashSet[String] = new HashSet()
	  
		//3. perform an intersection on selected item features, generate <intersectIF>
	    var intersectItemFeature:HashSet[String] = new HashSet()
		
		//4. generate ID string 
		val resourceStr = assembleContinuousDataIden(intersectUserFeature,intersectItemFeature)
	  
		//check if the regression data has already generated in jobInfo.jobStatus
		//  it is possible this combination has been used (and thus generated) by other classifiers. 
		//  in that case directly return resourceStr. 
		if (jobInfo.jobStatus.resourceLocation_AggregateData_Continuous.isDefinedAt(resourceStr)){
		  
			//5. perform a filtering on ( UserID, ItemID, rating) using <intersectUF> and <intersectIF>, 
			//   and generate <intersectTuple>
		  
		  
			 
			   
			//6. join features and <intersectTuple> and generate aggregated data (UF1 UF2 ... IF1 IF2 ... , feedback )
			val assembleFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_all"
		
			// join features and store in assembleFileName
			
			
			//7. save resource to <jobInfo.jobStatus.resourceLocation_AggregateData_Continuous>
			jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(resourceStr) = assembleFileName 
		}
		 
	    return resourceStr
	}
	
	def assembleContinuousDataIden(userFeature:HashSet[String], itemFeature:HashSet[String]):String = {
		return "ContAggData_" + HashString.generateHash(userFeature.toString) + "_"  + HashString.generateHash(itemFeature.toString) 
	}
	
	def assembleBinaryData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double):String = {
	    //see assembleContinuousData
		return null
	}
	
	def assembleBinaryDataIden(userFeature:HashSet[String], itemFeature:HashSet[String]):String = {
		return "BinAggData_" + HashString.generateHash(userFeature.toString) + "_"  + HashString.generateHash(itemFeature.toString)
	}
}