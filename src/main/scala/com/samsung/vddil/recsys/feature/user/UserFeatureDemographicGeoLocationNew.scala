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

		val demographicsinfo = sc.textFile(demogrphicsLoc + "/" + "demographics_zipcode_new2.txt")		
		Logger.info("We have found " + demographicsinfo.count() + " lines in demographics.")			
		
		var demographicHashTable = demographicsinfo.filter{
		    line => hasAllFields(line.split("\t"),line.split("\t").size == 55)
		}.map{
            line            => 
            val fields      = line.split("\t")
            var zipcode     = fields(0)
            var demoFeature = fields.tail.map{_.toDouble}
            (zipcode,demoFeature)
        }.collectAsMap
        
		Logger.info("Lets start to build geo features on " + trainDate.mkString(","))
		val geoDataOneDay = sc.textFile(geoLoc + "/" + trainDate(0))
		Logger.info("We have found " + geoDataOneDay.count() + " lines in day " + trainDate(0) + " .")
		
		val userFeatureMap = trainDate.map{
            date => 
			val geoDataOneDay = sc.textFile(geoLoc + "/" + date)		
			Logger.info("We have found " + geoDataOneDay.count() + " lines in day " + date + " .")	
			
			var userFeatureMapOneDay = geoDataOneDay.filter{
			    line        =>
			    val fields  = line.split("\t")
			    var duid    = fields(0)
			    var zipcode = fields(5) // the sixth fields in the duid_geo.tsv
			    demographicHashTable.isDefinedAt(zipcode)
			}.map{
			    line        =>
			    val fields  = line.split("\t")
			    var duid    = fields(0)
			    var zipcode = fields(5)		    
			    (duid,demographicHashTable(zipcode))
			}
			userFeatureMapOneDay
        }.reduce{ (a,b) =>
            a.union(b)
        }.distinct.collect
        
        for (tt <- userFeatureMap) {
            println(tt._1 + " " + tt._2.toSeq.toString)
        }
        
        println("featureFileName : " + featureFileName)
        println("resourceIden : " + resourceIden)
        println("featureMapFileName : " + featureMapFileName)
        
        
	    // 4. Generate and return a FeatureResource that includes all resources.  
        //save user geo demographic features as textfile
        /*
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	userFeatureMap.saveAsObjectFile(featureFileName) //directly use object + serialization. 
        }		
		
		Logger.info("saved! featureFileName is " + featureFileName)

		val featureStruct:UserFeatureStruct = 
  			new UserFeatureStruct(IdenPrefix, resourceIden, featureFileName, featureMapFileName)
		
		new FeatureResource(true, Some(resourceMap), resourceIden)
		* 
		*/
		FeatureResource.fail
    }
    val IdenPrefix:String = "UserFeatureGeo"
}