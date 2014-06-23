package com.samsung.vddil.recsys.model

import com.samsung.vddil.recsys.Logger
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.regression.RegressionModelRidge

object RegressionModelHandler extends ModelHandler {
	//predefined values for model name
	val RegModelRidge:String = "ridge_reg"
	val RegModelLasso:String = "lasso_reg"
	
	def buildModel(modelName:String, modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob): Boolean = {
	    
		Logger.logger.info("Processing regression model [%s:%s]".format(modelName, modelParams))
	  
	    var resource:ModelResource = ModelResource.fail
		modelName match{
		  case RegModelRidge => resource = RegressionModelRidge.learnModel(modelParams, dataResourceStr, jobInfo)
		  //case RegModelLasso => resource = ModelResource.fail
		  case _ => Logger.logger.warn("Unknown regression model name [%s]".format(modelName))
		}
	  
		//For the successful ones, push resource information to jobInfo.jobStatus
		if(resource.success){
		   resource.resourceMap(ModelResource.ResourceStr_RegressModel) match{
		     case model:ModelStruct => 
		       jobInfo.jobStatus.resourceLocation_RegressModel(resource.resourceIden) = model
		   }
		}
	    
		// return if the resource is successful or not.
	    resource.success
	}
	
	
}