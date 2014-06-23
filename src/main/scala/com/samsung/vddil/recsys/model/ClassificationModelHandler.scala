package com.samsung.vddil.recsys.model

import com.samsung.vddil.recsys.Logger
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.job.RecJob

object ClassificationModelHandler extends ModelHandler {
	val ClsModelLogisticL2:String = "lrl2_cls"
	val ClsModelLogisticL1:String = "lrl1_cls"
  
	def buildModel(modelName:String, modelParams:HashMap[String, String], dataResourceStr:String, jobInfo:RecJob): Boolean = {
	    
		Logger.logger.info("Processing classification model [%s:%s]".format(modelName, modelParams))
	  
	    var resource:ModelResource = ModelResource.fail
		modelName match{
		  //case ClsModelLogisticL2 => resource = ModelResource.fail
		  //case ClsModelLogisticL1 => resource = ModelResource.fail
		  case _ => Logger.logger.warn("Unknown classification model name [%s]".format(modelName))
		}
	  
		//For the successful ones, push resource information to jobInfo.jobStatus
		if(resource.success){
		   resource.resourceMap(ModelResource.ResourceStr_ClassifyModel) match{
		     case model:ModelStruct => 
		       jobInfo.jobStatus.resourceLocation_ClassifyModel(resource.resourceIden) = model
		   }
		}
	    
		// return if the resource is successful or not.
	    resource.success
	}
}