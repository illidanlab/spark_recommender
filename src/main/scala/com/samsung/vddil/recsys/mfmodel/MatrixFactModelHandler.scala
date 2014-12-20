package com.samsung.vddil.recsys.mfmodel

import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.job.RecMatrixFactJob
import scala.collection.mutable.HashMap

object MatrixFactModelHandler {
	val MatrixFactModelNMF:String = "nmf_model"
	val MatrixFactModelPMF:String = "pmf_model"
	    
	def buildModel(
		modelName:String,
		modelParams:HashMap[String, String],
		ratingData:CombinedDataSet,
		jobInfo:RecMatrixFactJob
	): Option[MatrixFactModel] = {
	    
	    
	    
	    None
	}
}