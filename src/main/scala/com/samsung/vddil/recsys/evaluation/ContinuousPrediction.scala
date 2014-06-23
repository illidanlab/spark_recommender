package com.samsung.vddil.recsys.evaluation

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
        diffSum/count
	}	
}