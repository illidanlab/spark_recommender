package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.model.regression.RegressionModelRidge
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model.regression.RegressionModelFactorizationMachine

object RegressionModelHandler extends ModelHandler {
	//predefined values for model name
	val RegModelRidge:String = "ridge_reg"
	val RegModelLasso:String = "lasso_reg"
	val RegModelFML2:String  = "fm_l2_reg"
	
	def buildModel(modelName:String, modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob): Boolean = {
	    
		Logger.info("Processing regression model [%s:%s]".format(modelName, modelParams))
	  
	    var resource:ModelResource = ModelResource.fail
		modelName match{
		  case RegModelRidge => resource = RegressionModelRidge.learnModel(modelParams, dataResourceStr, jobInfo)
		  //case RegModelLasso => resource = ModelResource.fail
		  case RegModelFML2  => resource = RegressionModelFactorizationMachine.learnModel(modelParams, dataResourceStr, jobInfo)
		  case _ => Logger.warn("Unknown regression model name [%s]".format(modelName))
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