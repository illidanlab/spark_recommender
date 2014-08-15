package com.samsung.vddil.recsys.testing

import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.utils.Logger

/**
 * This object handles the testing  
 */
object TestingHandler {
	//
	//val TestTypePrevItemPrecRecall = "prevItem_precrecall"
	//val TestTypeColdStartPrecRecall = "coldStart_precrecall"
  
	/**
	 * This function handles the testing of a given model.
	 */
	def testModel(testStruct:RecJobTest, model:ModelStruct) {
		
		testStruct match {
		    case RecJobTestNoCold(testName, testParams, metricList) => {
		        
		    }
		    
		    case RecJobTestColdItem(testName, testParams, metricList) => {
		        
		    }
		    
		    case _ => 
		        Logger.error("Test type not supported")
		}
	}
	
}