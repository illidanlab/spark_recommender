package com.samsung.vddil.recsys.feature.user

import com.samsung.vddil.recsys.job.RecJob
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.SparkContext
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.job.JobWithFeature

/*
 * User Feature: Geo Location features. 
 */
object UserFeatureDemographicGeoLocation extends FeatureProcessingUnit {
    
    val Param_numLatitudeSeg    = "numLatitudeSeg" 
    val Param_numLongitudeSeg   = "numLongitudeSeg" 
    val default_numLatitudeSeg  = 5 
    val default_numLongitudeSeg = 12
    // Define constants for the range of latitude and longitude of USA
    val _MIN_Latitude           = 24
    val _MAX_Latitude           = 50
    val _MIN_Longitude          = 66
    val _MAX_Longitude          = 125
    
    
	def processFeature(
	        featureParams:HashMap[String, String], 
	        jobInfo:JobWithFeature):FeatureResource = {
		//Logger.error("%s has not been implmented.".format(getClass.getName()))
		
		// 1. Load defined parameters or use default parameters
		// Take parameters, numLatitudeSeg, numLongitudeSeg
        var numLatitudeSeg  = default_numLatitudeSeg
        var numLongitudeSeg = default_numLongitudeSeg
        if(featureParams.isDefinedAt(Param_numLatitudeSeg)){
            numLatitudeSeg  = featureParams(Param_numLatitudeSeg).toInt
        }
        if(featureParams.isDefinedAt(Param_numLongitudeSeg)){
            numLongitudeSeg = featureParams(Param_numLongitudeSeg).toInt
        }

		
	    // 2. Generate resource identity using resouceIdentity()
		val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
		val resourceIden   = resourceIdentity(featureParams, dataHashingStr)

        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "_Map"		

		
		
		
	    // 3. Feature generation algorithms (HDFS operations)
		val sc:SparkContext = jobInfo.sc
				
		val geoLoc:String   = jobInfo.resourceLocAddon(RecJob.ResourceLocAddon_GeoLoc)
		
		val trainDate       = jobInfo.trainDates
		
		Logger.info("Geo data location: " + geoLoc)
		Logger.info("Lets start to build geo features on " + trainDate.mkString(","))
		
		val quantizationLatitude:Double  = (_MAX_Latitude - _MIN_Latitude).toDouble / numLatitudeSeg
		val quantizationLongitude:Double = (_MAX_Longitude - _MIN_Longitude).toDouble / numLongitudeSeg
		val totalSegments:Int            = numLatitudeSeg * numLongitudeSeg
		println("quantizationLatitude : " + quantizationLatitude)
		println("quantizationLongitude : " + quantizationLongitude)		
		
		
		val geoDataDay1 = sc.textFile(geoLoc + "/" + trainDate(0))		
		Logger.info("We have found " + geoDataDay1.count() + " lines in day " + trainDate(0) + " .")	
		
		var geoFeatureMap = geoDataDay1.map{
		    line => val fields = line.split('\t')
		//for (line <- geoDataDay1) {
		    //val fields = line.split('\t')
		    //println("fields are " + fields.mkString(" "))
		    
		    val latitudeSegmentIndex:Int = ((fields(1).toDouble - _MIN_Latitude) / quantizationLatitude).toInt
		    val longitudeSegmentIndex:Int = ((-1 * fields(2).toDouble - _MIN_Longitude) / quantizationLongitude).toInt		        

		    var overallIndex = 0
		    if (latitudeSegmentIndex < 0 || longitudeSegmentIndex < 0) {
		        overallIndex = 0 // out of the main continent of USA
		    }
		    else if (latitudeSegmentIndex > 4 || longitudeSegmentIndex > 11) {
		        overallIndex = 59 // out of the main continent of USA
		    }		
		    else {
    		    overallIndex = (latitudeSegmentIndex * numLongitudeSeg) + longitudeSegmentIndex // horizontally indexing		    		
	    	}
		    // generate sparse feature vector
		    val featureVecPair = Array((overallIndex, 1.0))
		    val featureVec = Vectors.sparse(totalSegments, featureVecPair)
		    (fields(0), featureVec)
		}
		
		for (trDate <- trainDate.tail) {
		    val geoDataDayRest = sc.textFile(geoLoc + "/" + trDate)	
    		Logger.info("We have found " + geoDataDayRest.count() + " lines in day " + trDate + " .")	
			val geoFeatureMapRest = geoDataDayRest.map{
			    line => val fields = line.split('\t')
			    
			    val latitudeSegmentIndex:Int = ((fields(1).toDouble - _MIN_Latitude) / quantizationLatitude).toInt
			    val longitudeSegmentIndex:Int = ((-1 * fields(2).toDouble - _MIN_Longitude) / quantizationLongitude).toInt		        
	
			    var overallIndex = 0
			    if (latitudeSegmentIndex < 0 || longitudeSegmentIndex < 0) {
			        overallIndex = 0 // out of the main continent of USA
			    }
			    else if (latitudeSegmentIndex > 4 || longitudeSegmentIndex > 11) {
			        overallIndex = 59 // out of the main continent of USA
			    }		
			    else {
	    		    overallIndex = (latitudeSegmentIndex * numLongitudeSeg) + longitudeSegmentIndex // horizontally indexing		    		
		    	}
			    // generate sparse feature vector
			    val featureVecPair = Array((overallIndex, 1.0))
			    val featureVec = Vectors.sparse(totalSegments, featureVecPair)
			    (fields(0), featureVec)
			}		    
		    geoFeatureMap = geoFeatureMap.union(geoFeatureMapRest).distinct
		}
		
		
	    // 4. Generate and return a FeatureResource that includes all resources.  
        //save user geo features as textfile
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	geoFeatureMap.saveAsObjectFile(featureFileName) //directly use object + serialization. 
        }		
		
		Logger.info("featureFileName is " + featureFileName)
		FeatureResource.fail
	}
    
    
	val IdenPrefix:String = "UserFeatureGeo"
}