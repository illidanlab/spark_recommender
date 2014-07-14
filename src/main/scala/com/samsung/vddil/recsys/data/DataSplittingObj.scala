package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import org.apache.spark.HashPartitioner
import com.samsung.vddil.recsys.linalg.Vector

/**
 * The object version of data splitting, which splits an object version assembled data into 
 * training, testing and validation, and stores the splitting in the data structure 
 * 
 * TODO: multiple random splitting: allowing more than one splittings.
 */
object DataSplittingObj {
    /**
     * Split data with continuous output.
     *  
     *  @param jobInfo:RecJob   The RecJob data structure that includes all necessary information 
     *  			     		related to a recommendation job. 
 	 *  @param resourceStr      A string that represents the data. Note that this string should be 
 	 *                   		IDENTICAL to the resource string of the data to be split.  
 	 *  
 	 *  @param trainingPerc     training data percentage
 	 *  @param testingPerc      testing data percentage
 	 *  @param validationPerc   validation data percentage
 	 *  
     */	 
    def splitContinuousData(jobInfo:RecJob, resourceStr:String, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double) = {
    	require(trainingPerc>=0   && trainingPerc<=1)
    	require(testingPerc>=0    && testingPerc<=1)
    	require(validationPerc>=0 && validationPerc<=1)
    	
    	require(trainingPerc + testingPerc + validationPerc <= 1)
    	//TODO: do we need also enforce the entire data to be used? see the require below. 
    	//require(trainingPerc + testingPerc + validationPerc >= 0.99)
    	
    	// check if the resource has already implemented. 
    	if ( ! jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train.isDefinedAt(resourceStr)){
    		//construct file names (locations). 
	    	val trDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_tr"
	    	val teDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_te"
	    	val vaDataFilename = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_va"
	    	
	    	if(jobInfo.skipProcessing(Array(trDataFilename, teDataFilename, vaDataFilename))){
	    	    //if all resources are available, we don't need to re-generate it. 
	    		Logger.info("All resources exist, and processing skipped.")
	    		//TODO: load them to RDD references and store somewhere. 
	    	}else{
	    	    //Generate resources.
	    	    val sc = jobInfo.sc
	    	    
	    	    //get the data file
		    	val assembleContinuousDataFile = 
		    	    jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(resourceStr).location
		    	
		    	//get partitioner.
		    	val numPartitions = Pipeline.getPartitionNum
		    	val partitioner = Pipeline.getHashPartitioner()
		    	
		    	//randomize the passed data
		    	val randData = sc.objectFile[(String, String, Vector, Double)](assembleContinuousDataFile
		    	        ).map { tuple =>
				    		//generate a random id for splitting 
				    		//First, get a random string, in current case get the first 
				    		//field of this data
				    		val randStr = tuple._1
				    		//generate a number between 0 to 9 by dividing last character by 10
				    		var randId = randStr(randStr.length-1).toInt % 10
				    		//divide it by 10 to make it lie between 0 to 1, 
				    		//to make it comparable to split percentages
				    		(randId.toDouble / 10, tuple)
		    	}.partitionBy(new HashPartitioner(numPartitions))
		    	//now the RDD becomes (Double (String, String, Vector, Double))
		    	
		    	//persist the randomize data for faster split generation
		    	randData.persist
		    	
		    	//split the data based on specified percentage
		    	
		    	//get the train data, i.e all random id < trainPc
		    	val trainData = randData.filter(_._1 < trainingPerc)
		    	                        .map(_._2) //remove the random id get only the data
		    	                        .coalesce((numPartitions.toDouble*trainingPerc).toInt)
		    	val trainSize = trainData.count
		    	
		    	
		    	//get the test data i.e. all random id  > trainPc but < (trainPc+testPc)
		    	val testData = randData.filter(x => x._1 >= trainingPerc 
		    	                                && x._1 < (trainingPerc + testingPerc))
		    			                   .map(_._2) //remove the random id get only the data
		                             .coalesce((numPartitions.toDouble*testingPerc).toInt)
		        val testSize = testData.count
		        
		        
		        //get the validation data i.e. all randomId > (trainPc+testPc)
		    	val valData = randData.filter(x => x._1 >= (trainingPerc + testingPerc))
		    	                      .map(_._2) //remove the random id get only the data
		    	                      .coalesce((numPartitions.toDouble*validationPerc).toInt)
		    	val validSize = valData.count
		    	
		    	
		    	//save data into files
		    	if (jobInfo.outputResource(trDataFilename)) trainData.saveAsObjectFile(trDataFilename)
		    	if (jobInfo.outputResource(teDataFilename)) testData.saveAsObjectFile(teDataFilename)
		    	if (jobInfo.outputResource(vaDataFilename)) valData.saveAsObjectFile(vaDataFilename)
		    	
		    	//unpersist the persisted data to free up memory associated
		    	randData.unpersist(false)
		    	
		    	Logger.info("Training sample size: " + trainSize)
		    	Logger.info("Testing sample size: " + testSize )
		    	Logger.info("Validation sample size: " + validSize)
	    	}
	    	
	    	//save resource to jobStatus
		    jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Train(resourceStr) = trDataFilename
		    jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Test(resourceStr)  = teDataFilename
		    jobInfo.jobStatus.resourceLocation_AggregateData_Continuous_Valid(resourceStr) = vaDataFilename
		    
    	}
    }
    
     /**
     *  Split data with binary output. 
     *    
     *  @param jobInfo          The RecJob data structure that includes all necessary information 
     *  			            related to a recommendation job. 
 	 *  @param resourceStr      A string that represents the data. Note that this string should be 
 	 *                          IDENTICAL to the resource string of the data to be split.  
 	 *  
 	 *  @param trainingPerc     training data percentage
 	 *  @param testingPerc      testing data percentage
 	 *  @param validationPerc   validation data percentage
 	 *  
 	 *  @param trainingBalance  if we balance strategy, in case the training is not balance.   
     */
	def splitBinaryData(jobInfo:RecJob, dataResourceStr:String, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double, trainingBalance:Boolean) = {
		Logger.logger.error("Not implemented")
		throw new NotImplementedError("Not implemented")
	    //TODO: implement
	  
	}
}