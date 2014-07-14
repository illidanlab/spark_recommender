package com.samsung.vddil.recsys.testing

import com.samsung.vddil.recsys.job.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
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
                  
    val usersRDD = sc.parallelize(userMap.values.toList).map((_,1))

    //broadcast item sets to worker nodes
    val itemIdSet = itemMap.values.toSet
    val bISet = sc.broadcast(itemIdSet)
    
    testData.filter(rating => bISet.value(rating.item))
            .map{rating => 
              (rating.user, (rating.item, rating.rating))
            }.join(usersRDD
            ).map {x =>
              val user = x._1
              val item = x._2._1._1
              val rating = x._2._1._2
              Rating(user, item, rating)
            }
  }
	
	
}
