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
		var filtTestData = testData  
    
    //get userMap and itemMap
    val userMap = jobStatus.userIdMap 
    val itemMap = jobStatus.itemIdMap   
 
    val userIdSet = userMap.values.toSet
    val itemIdSet = itemMap.values.toSet
    
    //broadcast these sets to worker nodes
    val bUSet = sc.broadcast(userIdSet)
    val bISet = sc.broadcast(itemIdSet)
    
    testData.filter(rating => 
                    bUSet.value(rating.user) && bISet.value(rating.item))
  }
	
	
}
