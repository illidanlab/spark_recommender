package com.samsung.vddil.recsys.evaluation

import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.utils.Logger

object ContinuousPrediction {
	/*
	 * compute mean square error
	 */
	def computeMSE(labelAndPreds:RDD[(Double, Double)]): Double = {
	    if(labelAndPreds.count == 0){
	        -1
	    }else{
	        val (diffSum, count) = labelAndPreds.map { case(v,p) => 
	    			                                        //square, 1 (for count) 
	                                                        (math.pow((v-p),2), 1)
	                                                     }.reduce { (a, b)  =>
	                                                    	 //sum square, sum count
	                                                        (a._1 + b._1, a._2 + b._2)
	                                                     }
	        Logger.info("Diffsum: " + diffSum + " count: " + count)    
	        diffSum/count
	    }
	}	
	
	
	/*
     * compute mean square error
     */
    def computeRMSE(labelAndPreds:RDD[(Double, Double)]): Double = {
        if(labelAndPreds.count == 0){
            -1
        }else{
            val mse = computeMSE(labelAndPreds)
	        math.sqrt(mse)
        }
    }
    
}
