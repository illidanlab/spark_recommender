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
	    println("max normalization!")
        val maxWatchTimeProgramMap = input.map{
	        x => 
        	(x._2,x._3) // progID, watchTime
	    }.reduceByKey((a,b) => max(a,b))
	     .map{
	        x => 
	        (x._1,("",x._2)) //progID, "", watchTime
	    }
	    
	    val normalizedData = input.map(x => (x._2, (x._1,x._3)))
	    						  .join(maxWatchTimeProgramMap)
	    						  .map{
	        						 x => 
	        						 (x._2._1._1,x._1,x._2._1._2/x._2._2._2)
	    }
        
        normalizedData
	}
    
    def transformation_Total(input:RDD[(String, String, Double)], sc:SparkContext):RDD[(String, String,Double)] = {
        val totalWatchTimeUserMap = input.map{
            x =>
            (x._1,x._3)
        }
        .reduceByKey(_+_)
        .map{
            x => 
            (x._1,("",x._2))
        }
        
	    val normalizedData = input.map(x => (x._1, (x._2,x._3)))
	    						  .join(totalWatchTimeUserMap)
	    						  .map{
	        						 x => 
	        						 (x._1,x._2._1._1,x._2._1._2/x._2._2._2)
	    }
        
        normalizedData     
    }
}