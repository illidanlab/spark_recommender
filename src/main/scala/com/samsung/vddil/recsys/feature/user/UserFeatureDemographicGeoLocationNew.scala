package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.SparkContext
import com.samsung.vddil.recsys.linalg.Vectors
import org.apache.spark.SparkContext._
import scala.util.control._
import com.samsung.vddil.recsys.feature.UserFeatureStruct
import scala.util.Marshal
import scala.io.Source
import scala.collection.immutable
import java.io._
import scala.collection.Parallel
import org.apache.tools.ant.taskdefs.Parallel

/*
 * User Feature: Geo Location and Demographic features. 
 */
object UserFeatureDemographicGeoLocationNew extends FeatureProcessingUnit {
    def hasAllFields(line:Array[String],lengthCondition:Boolean):Boolean = {
        if (lengthCondition == false)
        	return false 
        for (tt <- line) {
            if (tt.isEmpty()) {
                return false
            }
            //var temp = tt.toDouble
            //if (!temp.isInstanceOf[Double])
            //    return false                
        }
        return true
    }
    
    def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		//Logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Load defined parameters or use default parameters
    	
	    // 2. Generate resource identity using resouceIdentity()
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		val resourceIden   = resourceIdentity(featureParams, dataHashingStr)

        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + resourceIden + "_Map"	
        
	    // 3. Feature generation algorithms (HDFS operations)
		val sc:SparkContext = jobInfo.sc
				
		val geoLoc:String   = jobInfo.resourceLocAddon(RecJob.ResourceLocAddon_GeoLoc)
		
		val trainDate       = jobInfo.trainDates        
		
		val demogrphicsLoc:String = "data/Demographics"
		    
		Logger.info("Geo data location: " + geoLoc)

		val demographicsinfo = sc.textFile(demogrphicsLoc + "/" + "demographics_zipcode_new_full.txt") // change to full version		
		Logger.info("We have found " + demographicsinfo.count() + " lines in demographics.")			
		
		var accum = sc.accumulator(0)
		
		var demographicHashTable = demographicsinfo.filter{
		    line => hasAllFields(line.split("\t"),line.split("\t").size == 55)
		}.map{
            line            => 
            val fields      = line.split("\t")
            var zipcode     = fields(0)
            var demoFeature = fields.tail.map{_.toDouble}
            accum += 1
            (zipcode,demoFeature)
        }.collectAsMap
        println("The demographic HashTable contains " + accum + " records.")
        val demographicHashTableRDD = sc.broadcast(demographicHashTable)
        
        /*
		Logger.info("Lets start to build geo features on " + trainDate.mkString(","))
		val geoDataOneDay = sc.textFile(geoLoc + "/" + trainDate(0))
		Logger.info("We have found " + geoDataOneDay.count() + " lines in day " + trainDate(0) + " .")
		* */
		
		val userFeatureMap = trainDate.map{
            date => 
			val geoDataOneDay = sc.textFile(geoLoc + "/" + date + "/duid_geo.tsv")		
			Logger.info("We have found " + geoDataOneDay.count() + " lines in day " + date + " .")	
			
			var userFeatureMapOneDay = geoDataOneDay.filter{
			    line        =>
			    val fields  = line.split("\t")
			    var duid    = fields(0)
			    var zipcode = fields(5) // the sixth fields in the duid_geo.tsv
			    demographicHashTableRDD.value.isDefinedAt(zipcode)
			}.map{
			    line        =>
			    val fields  = line.split("\t")
			    var duid    = fields(0)
			    var zipcode = fields(5)		    
			    (duid,demographicHashTableRDD.value(zipcode))
			}
			userFeatureMapOneDay
        }.reduce{ (a,b) =>
            a.union(b)
        }.distinct
        
        for (tt <- userFeatureMap) {
            println(tt._1 + " " + tt._2.toSeq.toString)
        }
        
        println("featureFileName : " + featureFileName)
        println("resourceIden : " + resourceIden)
        println("featureMapFileName : " + featureMapFileName)
        
       
        //save user geo demographic features as textfile
        val demographicsDescription = sc.textFile(demogrphicsLoc + "/" + "demographics_description.txt")
        val lenDescription = demographicsDescription.count.toInt
        val demographicsDescriptionMap = ((0 until lenDescription) zip demographicsDescription.collect).toMap
        val demographicsDescriptionMapRDD = sc.makeRDD(demographicsDescriptionMap.toList)
        for (tt <- (0 until lenDescription))
        	println(tt + " -> " + demographicsDescriptionMap(tt))
        
        
    	//save item features as textfile
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	userFeatureMap.saveAsObjectFile(featureFileName) //directly use object + serialization. 
        }		
        
        //save genre mapping to indexes
        if (jobInfo.outputResource(featureMapFileName)){
        	Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
        	demographicsDescriptionMapRDD.map{
        	    x => x._1 + "," + x._2
        	}.saveAsTextFile(featureMapFileName)
        }
        
        
    	// 4. Generate and return a FeatureResource that includes all resources.  
		val featureStruct:UserFeatureStruct = 
  			new UserFeatureStruct(IdenPrefix, resourceIden, featureFileName, featureMapFileName)
        	
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(FeatureResource.ResourceStr_UserFeature) = featureStruct
		Logger.info("Saved item features and feature map")	
		
		new FeatureResource(true, Some(resourceMap), resourceIden)
	
    }
    val IdenPrefix:String = "UserFeatureGeo"
}