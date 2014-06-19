package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.Logger

/**
 * This process splits an assembled data into training, testing and validation, and store 
 * the splittings in the data structure.  
 *    
 * TODO: multiple random splitting: allowing more than one splittings.
 */
object DataSplitting {
    /**
     * Split data with continuous output.
     *  
     *  jobInfo:RecJob   The RecJob data structure that includes all necessary information 
     *  			     related to a recommendation job. 
 	 *  resourceStr      A string that represents the data. Note that this string should be 
 	 *                   IDENTICAL to the resource string of the data to be split.  
 	 *  
 	 *  trainingPerc     training data percentage
 	 *  testingPerc      testing data percentage
 	 *  validationPerc   validation data percentage
 	 *  
     */
	def splitContinuousData(jobInfo:RecJob, resourceStr:String, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double) = {
	    
	    
	    // check if the resource has already implemented. 
	    if (jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train.isDefinedAt(resourceStr)){
	    	//construct file names (locations). 
	    	val trDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_tr"
	    	val teDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_te"
	    	val vaDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_va"
	    	
	    	//TODO: generate random numbers and map to files. 
	    	Logger.logger.error("Not implemented")
	    	
	    	//save resource to jobStatus
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train(resourceStr) = trDataFilename
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Test(resourceStr)  = teDataFilename
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Valid(resourceStr) = vaDataFilename
	    }
	}
  
    /**
     *  Split data with binary output. 
     *    
     *  jobInfo:RecJob   The RecJob data structure that includes all necessary information 
     *  			     related to a recommendation job. 
 	 *  resourceStr      A string that represents the data. Note that this string should be 
 	 *                   IDENTICAL to the resource string of the data to be split.  
 	 *  
 	 *  trainingPerc     training data percentage
 	 *  testingPerc      testing data percentage
 	 *  validationPerc   validation data percentage
 	 *  
 	 *  trainingBalance  if we balance strategy, in case the training is not balance.   
     */
	def splitBinaryData(jobInfo:RecJob, dataResourceStr:String, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double, trainingBalance:Boolean) = {
		Logger.logger.error("Not implemented")
	    //TODO: implement
	  
	}
    
}