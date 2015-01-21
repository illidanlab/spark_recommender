package com.samsung.vddil.recsys.data

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math.max
import com.samsung.vddil.recsys.utils.{HashString, Logger}


object DataTransformation {
    

    
    
    val dataTransformation_Max = "max"
	val dataTransformation_totalWatchTime = "total"
    
	def transformation_None(input:RDD[(String, String, Double)]):RDD[(String, String,Double)] = {
	    input
	}
    
    def transformation_Max(input:RDD[(String, String, Double)], sc:SparkContext):RDD[(String, String,Double)] = {
	    val maxWatchTimeProgramMap = input.map{
	        x => 
        	(x._2,x._3)
	    }.reduceByKey{
	        (a,b) => max(a,b)
	    }.collectAsMap
	    val bMaxWatchTimeProgramMap = sc.broadcast(maxWatchTimeProgramMap)
	    	    
	    val normalizedData = input.map{
	        x => 
	        val maxWT = bMaxWatchTimeProgramMap.value(x._2)
	        (x._1, x._2, x._3/maxWT)
	    }
        
        normalizedData
	}
    
    def transformation_Total(input:RDD[(String, String, Double)], sc:SparkContext):RDD[(String, String,Double)] = {
        val totalWatchTimeUserMap = input.map{
            x =>
            (x._1,x._3)
        }
        .reduceByKey(_+_)
        .collectAsMap
        
        Logger.info("totalWatchTimeUserMap has " + totalWatchTimeUserMap.size + " users")
                
        val bTotalWatchTimeUserMap = sc.broadcast(totalWatchTimeUserMap)
	    val normalizedData = input.map{
	        x => 
	        val totalWT = bTotalWatchTimeUserMap.value(x._1)
	        (x._1, x._2, x._3/totalWT)
	    }
        
        normalizedData        
    }
}