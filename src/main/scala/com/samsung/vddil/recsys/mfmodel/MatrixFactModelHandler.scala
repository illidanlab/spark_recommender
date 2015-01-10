package com.samsung.vddil.recsys.mfmodel

import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import scala.collection.mutable.HashMap

object MatrixFactModelHandler {
	val MatrixFactModelTypeNMF:String = "nmf_model"
	val MatrixFactModelTypePMF:String = "pmf_model"
	    
	def buildModel(
		modelName:String,
		modelParams:HashMap[String, String],
		ratingData:CombinedDataSet,
		jobInfo:RecMatrixFactJob
	): Option[MatrixFactModel] = {
	    
	    if(modelName.compareTo(MatrixFactModelTypePMF) == 0){
	        val modelGenerator = MatrixFactModelPMF(modelParams)
	        modelGenerator.train(ratingData, jobInfo)
	    }else{
	    	None
	    }
	    
	    
	}
}