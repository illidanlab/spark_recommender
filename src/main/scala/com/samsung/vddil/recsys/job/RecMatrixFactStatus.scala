package com.samsung.vddil.recsys.job

import com.samsung.vddil.recsys.data.CombinedDataSet

/** 
 * Stores the location of different types of resources (prepared data, features, models). 
 * 
 * @param jobInfo the matrix factorization recommendation job associated with this status. 
 */
case class RecMatrixFactStatus(jobInfo:RecMatrixFactJob) extends JobStatus {

	/*
	 * Data processing resources   
	 */ 
    var resourceLocation_CombinedData_train: Option[CombinedDataSet] = None    
    
    
    def allCompleted():Boolean = {
        true
    }    
    
    def showStatus():Unit = {
        
    }
}