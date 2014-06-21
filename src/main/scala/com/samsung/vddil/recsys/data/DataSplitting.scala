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
	    
		//NOTE: as floating comparison in general not accurate hence using 0.99
		//ideally it should be 1.0
		assert(trainingPerc + testingPerc + validationPerc >= 0.99)
	    
	    // check if the resource has already implemented. 
	    if ( ! jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train.isDefinedAt(resourceStr)){
	    	//construct file names (locations). 
	    	val trDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_tr"
	    	val teDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_te"
	    	val vaDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_va"
	    	
	    	//get the data file
	    	val assembleContinuousDataFile = jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(resourceStr)
	    	
	    	//get spark context
	    	val sc = jobInfo.sc
	    	
	    	//randomize the passed data
	    	val randData = sc.textFile(assembleContinuousDataFile).map { line =>
	    		//generate a random id for splitting 
	    		//First, get a random string, in current case get the first 
	    		//field of this data
	    		val randStr = line.split(',')(0)
	    		//generate a number between 0 to 9 by dividing last character by 10
	    		var randId = randStr(randStr.length-1).toInt % 10
	    		//divide it by 10 to make it lie between 0 to 1, 
	    		//to make it comparable to split percentages
	    		(randId.toDouble / 10, line)
	    	}
	    	
	    	//persist the randomize data for faster split generation
	    	randData.persist
	    	
	    	//split the data based on specified percentage
	    	
	    	//get the train data, i.e all random id < trainPc
	    	val trainData = randData.filter(_._1 < trainingPerc)
	    	                        .map(_._2) //remove the random id get only the data
	    	
	    	//get the test data i.e. all random id  > trainPc but < (trainPc+testPc)
	    	val testData = randData.filter(x => x._1 >= trainingPerc 
	    	                                && x._1 < (trainingPerc + testingPerc))
	    			               .map(_._2) //remove the random id get only the data
	        
	        //get the validation data i.e. all randomId > (trainPc+testPc)
	    	val valData = randData.filter(x => x._1 >= (trainingPerc + testingPerc))
	    	                      .map(_._2) //remove the random id get only the data
	    	
	        //save data into files
	    	trainData.saveAsTextFile(trDataFilename)
	    	testData.saveAsTextFile(teDataFilename)
	    	valData.saveAsTextFile(vaDataFilename)
	    	
	    	//save resource to jobStatus
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train(resourceStr) = trDataFilename
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Test(resourceStr)  = teDataFilename
	    	jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Valid(resourceStr) = vaDataFilename
	    	
	    	//unpersist the persisted data to free up memory associated
	    	randData.unpersist(false)
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