package com.samsung.vddil.recsys.evaluation

import com.samsung.vddil.recsys.Logger
import org.apache.spark.rdd.RDD

object ContinuousPrediction {
	/*
	 * compute mean square error
	 */
	def computeMSE(labelAndPreds:RDD[(Double, Double)]): Double = {
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
	
	
	/*
     * compute mean square error
     */
    def computeRMSE(labelAndPreds:RDD[(Double, Double)]): Double = {
        val mse = computeMSE(labelAndPreds)
        math.sqrt(mse)
    }
    
}
