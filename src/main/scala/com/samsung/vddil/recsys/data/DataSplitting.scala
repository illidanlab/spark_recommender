package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.mapred.JobInfo


/**
 * The object version of data splitting, which splits an object version assembled data into 
 * training, testing and validation, and stores the splitting in the data structure 
 * 
 * TODO: multiple random splitting: allowing more than one splittings.
 */
object DataSplitting {
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
 	 *  @return split name 
     */	 
    def splitContinuousData(jobInfo:RecJob, allData:AssembledDataSet, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double):String = {
        
    	require(trainingPerc>=0   && trainingPerc<=1)
    	require(testingPerc>=0    && testingPerc<=1)
    	require(validationPerc>=0 && validationPerc<=1)
    	
    	require(trainingPerc + testingPerc + validationPerc <= 1)
    	//TODO: do we need also enforce the entire data to be used? see the require below. 
    	//require(trainingPerc + testingPerc + validationPerc >= 0.99)
    	
    	val splitName = HashString.generateHash("split1_" + trainingPerc.toString + testingPerc.toString + validationPerc.toString)
    	val partitionNum = jobInfo.partitionNum_train
    	
    	// check if the resource has already implemented. 
    	if ( ! allData.getSplit(splitName).isDefined){
    	    
    		splitDataByPercentage(
    				splitName, jobInfo.sc, allData, 
    				(resourceLoc:String) => jobInfo.outputResource(resourceLoc),
    				partitionNum,
    				trainingPerc, 
    				testingPerc, 
    				validationPerc)
    	}
    	
    	splitName
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
	def splitBinaryData(jobInfo:RecJob, allData:AssembledDataSet, 
			trainingPerc:Double, testingPerc:Double, validationPerc:Double, trainingBalance:Boolean):String = {
		Logger.logger.error("Not implemented")
		throw new NotImplementedError("Not implemented")
	    //TODO: implement
	  
	}
	
	
	
	def splitDataByPercentage(
	        splitName:String, 
	        sc:SparkContext,
	        allData:AssembledDataSet, 
	        outputResource:String=>Boolean,
	        partitionNum:Int,
			trainingPerc:Double, 
			testingPerc:Double, 
			validationPerc:Double) = {
	    
		require(trainingPerc>=0   && trainingPerc<=1)
		require(testingPerc>=0    && testingPerc<=1)
		require(validationPerc>=0 && validationPerc<=1)
		require(trainingPerc + testingPerc + validationPerc <= 1)
	
		//NOTE: do we need also enforce the entire data to be used? see the require below. 
		//require(trainingPerc + testingPerc + validationPerc >= 0.99)
	    
	    val resourceStr = allData.resourceIden

		//construct file names (locations). 
	    val trDataResId    = allData.resourceIden + "_" + splitName + "_tr"
    	val trDataFilename = allData.resourceLoc  + "_" + splitName + "_tr"
    	
    	val teDataFilename = allData.resourceLoc  + "_" + splitName + "_te"
    	val teDataResId    = allData.resourceIden + "_" + splitName + "_te"
    	
    	val vaDataFilename = allData.resourceLoc  + "_" + splitName + "_va"
    	val vaDataResId    = allData.resourceIden + "_" + splitName + "_va"
    	
    	val trDataStruct   = allData.createSplitStruct(trDataResId, trDataFilename)
    	val teDataStruct   = allData.createSplitStruct(teDataResId, teDataFilename)
    	val vaDataStruct   = allData.createSplitStruct(vaDataResId, vaDataFilename)
    	
    	// check if the resource has already implemented. 
    	if(trDataStruct.resourceExist && teDataStruct.resourceExist && vaDataStruct.resourceExist){
    	    //if all resources are available, we don't need to re-generate it. 
    		Logger.info("All resources exist, and processing skipped.")
    	}else{
    	    //Generate resources.
	    	
	    	//get partitioner.
	    	val partitioner = Pipeline.getHashPartitioner(partitionNum)
	    	
	    	//randomize the passed data
	    	val randData = sc.objectFile[(Int, Int, Vector, Double)](allData.resourceLoc
	    	        ).map { tuple =>
			    		//generate a random id for splitting 
			    		//First, get a random int, in current case get the first 
			    		//field of this data
			    		//generate a number between 0 to 9 by dividing last character by 10
			    		var randId = tuple._1 % 10
			    		//divide it by 10 to make it lie between 0 to 1, 
			    		//to make it comparable to split percentages
			    		(randId.toDouble / 10, tuple)
	    	}
	    	//now the RDD becomes (Double (Int, Int, Vector, Double))

	    	
            val partedRandData = randData//.partitionBy(new RangePartitioner(numPartitions/4, randData)) 

	    	//persist the randomize data for faster split generation
	    	partedRandData.persist
	    	
	    	Logger.info("Number of partitions: " + partedRandData.partitions.length)


	    	//split the data based on specified percentage
	    	
	    	//get the train data, i.e all random id < trainPc
	    	val trainData = partedRandData.filter(_._1 < trainingPerc)
                                    .values //remove the random id get only the data
	    	
	    	//get the test data i.e. all random id  > trainPc but < (trainPc+testPc)
	    	val testData = partedRandData.filter(x => x._1 >= trainingPerc 
	    	                                && x._1 < (trainingPerc + testingPerc))
                                   .values //remove the random id get only the data
	        
	        //get the validation data i.e. all randomId > (trainPc+testPc)
	    	val valData = partedRandData.filter(x => x._1 >= (trainingPerc + testingPerc))
                                  .values //remove the random id get only the data
	    	
	    	//save data into files
	    	if (outputResource(trDataFilename)) trainData.saveAsObjectFile(trDataFilename)
	    	if (outputResource(teDataFilename)) testData.saveAsObjectFile(teDataFilename)
	    	if (outputResource(vaDataFilename)) valData.saveAsObjectFile(vaDataFilename)
	    	
	    	trDataStruct.size = trainData.count
	    	teDataStruct.size = testData.count
	    	vaDataStruct.size = valData.count
	    	
	    	//unpersist the persisted data to free up memory associated
	    	randData.unpersist(false)
    	}
		
		//set split
		allData.putSplit(splitName, trDataStruct, teDataStruct, vaDataStruct)
    }
}
