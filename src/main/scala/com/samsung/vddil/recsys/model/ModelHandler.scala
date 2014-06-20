package com.samsung.vddil.recsys.model

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap

/**
 * The model handler implementations are used to process a specified type of model.  
 * 
 * @author jiayu.zhou
 */
trait ModelHandler {
	/*
	 * This method process one type of model, and return a boolean value indicating if the model 
	 * is built successfully or failed. For the successful ones, this method should push resource 
	 * information to jobInfo.jobStatus 
	 */
	def buildModel(modelName:String, modelParam:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob): Boolean
}