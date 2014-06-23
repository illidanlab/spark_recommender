package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import org.apache.hadoop.mapred.JobInfo
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.utils.HashString
import org.apache.spark.SparkContext


/**
 * This is a trait for model processing unit. 
 * 
 * Each model processing unit is an object that 
 * provides a static method process model
 *  
 *  @author jiayu.zhou
 */
trait ModelProcessingUnit {
  
	/*
	 * Each model process method takes the model parameters, a data resource identity and job info, 
	 * build the model and then store access information (the location of the model in HDFS), and 
	 * results (success/fail) in the ModelResource data structure.  
	 * 
	 * General steps. 
	 * 1. Complete default parameters 
	 * 2. Generate resource identity using resouceIdentity()
	 * 3. Model learning algorithms (HDFS operations)
	 * 4. Compute training and testing error.
	 * 5. Generate and return a ModelResource that includes all resources.  
	 * 
	 */
	def learnModel(modelParams:HashMap[String, String], dataResourceStr:String, JobInfo:RecJob):ModelResource

	/*
	 * Given a model parameter map, this method provides a string that uniquely determines this model.
	 * Note that this resource identity should include all parameters in order to generate a string. Therefore
	 * default values not specified in job should be completed in the featureParam before sending to resource identity.
	 * 
	 * dataResourceIden is the identity of data used to train/test/validate the model
	 */
	val IdenPrefix:String
	def resourceIdentity(modelParams:HashMap[String, String], dataResourceStr:String):String = {
	    IdenPrefix + "_" + HashString.generateHash(modelParams.toString) + "_" + dataResourceStr
	}
	
	def checkIdentity(idenString:String):Boolean = {
	    idenString.startsWith(IdenPrefix)
	}
	
	
	//The definition of the following two methods are moved to ModelStruct. 
	//def saveModel(modelFileName: String, sc: SparkContext)
	//def getModel(modelFileName: String, sc: SparkContext)
}