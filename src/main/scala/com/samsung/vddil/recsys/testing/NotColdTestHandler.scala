package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.job.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.samsung.vddil.recsys.job.RecJobStatus

trait NotColdTestHandler {
	
	
	/*
	 * remove new users and items from test
	 */
	def filterTestRatingData(testData: RDD[Rating], jobStatus: RecJobStatus,
			                    sc:SparkContext): RDD[Rating] = {
		val userSet = jobStatus.users.toSet
		val itemSet = jobStatus.items.toSet
		testData.filter(rating => userSet(rating.user) && itemSet(rating.item))
	}
	
	
}