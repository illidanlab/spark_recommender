package com.samsung.vddil.recsys.job

import com.samsung.vddil.recsys.data.CombinedDataSet
import com.samsung.vddil.recsys.data.CombinedRawDataSet
import com.samsung.vddil.recsys.mfmodel.MatrixFactModel
import scala.collection.immutable.{HashMap => IHashMap}
import scala.collection.mutable.{HashMap => MHashMap}
import com.samsung.vddil.recsys.testing.TestUnit
import com.samsung.vddil.recsys.testing.TestUnitMatrixFact
import com.samsung.vddil.recsys.utils.Logger

/** 
 * Stores the location of different types of resources (prepared data, features, models). 
 * 
 * @param jobInfo the matrix factorization recommendation job associated with this status. 
 */
case class RecMatrixFactStatus(jobInfo:RecMatrixFactJob) 
	extends JobStatusWithFeature with JobStatusWithCombinedData {

	/*
	 * Data processing resources   
	 */ 
    var resourceLocation_models: IHashMap[String, MatrixFactModel] = IHashMap() 
    
    val completedTests:
    	MHashMap[MatrixFactModel, 
    	         MHashMap[TestUnitMatrixFact, TestUnit.TestResults]] = new MHashMap()
    
    def allCompleted():Boolean = {
        true
    }    
    
    def showStatus():Unit = {
        Logger.logger.info("Completed Item Features " + completedItemFeatures)
    	Logger.logger.info("Completed User Features " + completedUserFeatures)
    	
    }
}