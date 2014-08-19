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
import scala.collection.immutable.Vector

/*
 * User Feature: Geo Location and Demographic features. 
 */
object UserFeatureDemographicGeoLocation extends FeatureProcessingUnit {
    def hasAllFields(line:Array[String],lengthCondition:Boolean):Boolean = {
        if (lengthCondition == false)
        	return false 
        for (tt <- line) {
            if (tt.isEmpty()) {
                return false
            }           
        }
        return true
    }
    
    def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		//Logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Load defined parameters or use default parameters
        // No pre-defined parameters
    	
	    // 2. Generate resource identity using resouceIdentity()
		val dataHashingStr     = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		
		val resourceIden       = resourceIdentity(featureParams, dataHashingStr)

        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + "/" + resourceIden + "_Map"	
        
	    // 3. Feature generation algorithms (HDFS operations)
		val sc:SparkContext = jobInfo.sc
				
		val geoLoc:String   = jobInfo.resourceLocAddon(RecJob.ResourceLocAddon_GeoLoc)
		Logger.info("Geo data location: " + geoLoc)
		
		val trainDate       = jobInfo.trainDates        
		
		// Predefined address for off-data (demographics information and feature descriptions)
		val demogrphicsLoc:String = "/user/yin.zhou/recsys_offline_data" // "data/Demographics"		    

		val demographicsinfo = sc.textFile(demogrphicsLoc + "/" + "demographics_zipcode_new_full.txt") // change to full version		
		Logger.info("We have found " + demographicsinfo.count() + " lines in demographics information table.")			
		
		var accum = sc.accumulator(0)
		
		// Load the demographics information and convert it to a hashmap
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
        Logger.info("The demographic HashTable contains " + accum + " records.")
        
        val demographicHashTableRDD = sc.broadcast(demographicHashTable) // broadcast the hash table for fast generating user feature
        
        // Load the duid_geo.tsv at each date and use the zipcode to hash the demographics information
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
			    val fields   =             line.split("\t")
			    var duid     =             fields(0)
			    var zipcode  =             fields(5)		    
			    (duid,demographicHashTableRDD.value(zipcode))
			}
			userFeatureMapOneDay
        }.reduce{ (a,b) =>
            a.union(b)
        }.distinct
        
        val uMap   = sc.parallelize(jobInfo.jobStatus.userIdMap.toList)
        val userF  = userFeatureMap.join(uMap)
        val userFeatureMap2Sparse = userF.map{
            x => (x._2._2, Vectors.sparse(x._2._1))
        }
  
        
        println("featureFileName : " + featureFileName)
        println("resourceIden : " + resourceIden)
        println("featureMapFileName : " + featureMapFileName)
        
       
        //save user geo demographic features as textfile and load off-line feature descriptions
        val demographicsDescription       = sc.textFile(demogrphicsLoc + "/" + "demographics_description.txt")
        val lenDescription                = demographicsDescription.count.toInt
        val demographicsDescriptionMap    = ((0 until lenDescription) zip demographicsDescription.collect).toMap
        val demographicsDescriptionMapRDD = sc.makeRDD(demographicsDescriptionMap.toList)
        
               
    	//save demographic user features as ObjectFile
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	userFeatureMap2Sparse.saveAsObjectFile(featureFileName) //directly use object + serialization. 
        }		
                       
        
        //save feature description mapping to indexes
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