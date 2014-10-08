package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashMap
import scala.util.control.Breaks._


object ItemFeatureChannel extends FeatureProcessingUnit with ItemFeatureExtractor {
    val IdenPrefix:String = "ItemFeatureChannel"
	var trFeatureParams = new HashMap[String,String]()
	
	def getFeatureSources(dates:List[String], jobInfo:RecJob):List[String] = {
    	dates.map{date =>
      		jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date + "/schedule*"
    	}.toList
    }
	
    def loadSourceMap(featureMapFileName:String, sc:SparkContext):
    		scala.collection.Map[String,Int]= {
	    sc.textFile(featureMapFileName).map{line=>
		    val splits = line.split(",")
		    val sourceFeatureId:Int = splits(0).toInt
		    val sourceId:String     = splits(1)
		    (sourceId, sourceFeatureId)
		}.collectAsMap
    }
    
    def constructChannelFeature(
            scheduleFiles:List[String], 
            items:Set[String],
            sourceMap: scala.collection.Map[String,Int],
            sc:SparkContext): RDD[(String, Vector)] = {
        
        val bItemSet = sc.broadcast(items)
        scheduleFiles.map{scheduleFile =>
				val scheduleRDD = sc.textFile(scheduleFile).map{line =>
				    val fields = line.split('|')
				    val pid: String       = fields(ScheduleField_ProgId)
				    val channelId: String = fields(ScheduleField_ChannelId)
				    (pid, Set(channelId))
				}.filter{scheduleEntry =>
				    bItemSet.value.contains(scheduleEntry._1)
				}
				scheduleRDD
			}.reduce{(a,b) =>
			    a.union(b)
			}.reduceByKey{(a,b) =>
			    a ++ b
			}.map{line =>
			    val pid:String     = line._1
			    val feature:Vector = generateFeatureVector(line._2, sourceMap)
			    (pid, feature)
			}
    }
    
	def extractFeature(
            items:Set[String], 
            featureSources:List[String],
    		featureParams:HashMap[String, String], 
    		featureMapFileName:String, 
    		sc:SparkContext): RDD[(String, Vector)] = {
	   
	   //from RDD to in-memory map for feature construction.
	   val sourceMap: scala.collection.Map[String,Int] = 
	         loadSourceMap(featureMapFileName, sc)
	   //construct features according to the feature map. 
	   val programChannel: RDD[(String, Vector)]
			= constructChannelFeature(featureSources,items,sourceMap, sc)
			
		programChannel
	}
	
	val SourceId   = 1
	val SourceName = 2
	val SourceDesc = 3
	val ScheduleField_ProgId = 4
	val ScheduleField_ChannelId = 1
	
	def processFeature(
	        featureParams:HashMap[String, String], 
	        jobInfo:RecJob):FeatureResource = {
		val trainCombData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
		
		val sc = jobInfo.sc
		
		//1. Complete default parameters 
		
		
		//2. Generate resource identity
		val dataHasingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
        val resourceIden = resourceIdentity(featureParams, dataHasingStr)
        
        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "_Map"
        							
        //3. Feature generation algorithm
        //get set of items 
        val itemSet = trainCombData.getItemList().collect.toSet
        
        val scheduleFiles = jobInfo.trainDates.map{trainDate =>
            jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/schedule*"
        }
		
		val sourceFiles   = jobInfo.trainDates.map{trainDate =>
		    jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/source.txt.gz"
		}

		//Construct feature map if necessary. 
		if(jobInfo.outputResource(featureMapFileName)){
		    //construct a RDD (featureIdx, sourceId, sourceDesc)
			val sourceMapRDD:RDD[(Int, String, String)] = 
			    sourceFiles.map{sourceFile => 
			        val curSourceMap:RDD[(String, String)] =
			            sc.textFile(sourceFile).map{line =>
			            	val fields = line.split('|')
			            	val sourceId = fields(SourceId)
			            	val sourceName = fields(SourceName)
			            	(sourceId, sourceName)
			        }
			        curSourceMap
			    }.reduce{(a, b) =>
			        a.union(b)
			    }.reduceByKey{ 
			        //here is a very tricky part, one source may 
			        //correspond to multiple description.
			        //STRATEGY: randomly choose one using reduce. 
			        (a:String,b:String)=> a
			    }.zipWithIndex.map{line =>
			        val sourceFeatureId = line._2.toInt
			        val sourceId        = line._1._1
			        val sourceDesc      = line._1._2
			        (sourceFeatureId, sourceId, sourceDesc)
			    }
			    
			Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
			Logger.logger.info("SourceMap Size: " + sourceMapRDD.count)
			
			sourceMapRDD.map{line =>
				val sourceFeatureId = line._1
				val sourceId        = line._2
				val sourceDesc      = line._3
				sourceFeatureId + "," + sourceId + "," + sourceDesc
			}.saveAsTextFile(featureMapFileName)
		}
		
		//from RDD to in-memory map for feature construction.
		val sourceMap: scala.collection.Map[String,Int] = 
		    loadSourceMap(featureMapFileName, sc)
		
	    Logger.logger.info("Loaded SourceMap Size: " + sourceMap.size)
		    
		//Generate features if necessary
		if(jobInfo.outputResource(featureFileName)){
		    
		    val itemSet = trainCombData.getItemList().collect.toSet
		    
		    //generate feature vector for each item
			val programChannel: RDD[(String, Vector)]
					= constructChannelFeature(scheduleFiles.toList, itemSet,sourceMap, sc)
		    
			val itemIdMap = trainCombData.getItemMap().collectAsMap
			val bItemMap = sc.broadcast(itemIdMap)
				
			programChannel.map{programChannelEntry =>
			    val itemId:Int = bItemMap.value(programChannelEntry._1)
			    val feature:Vector = programChannelEntry._2
			    (itemId, feature)
			}.saveAsObjectFile(featureFileName)
			Logger.info("Saved item features")
		}
		
		// 4. Generate and return a FeatureResource that includes all resources.
		val featureStruct:ItemFeatureStruct = 
		    new ItemFeatureStruct(
		            IdenPrefix, resourceIden, featureFileName, 
		            featureMapFileName, featureParams, ItemFeatureGenre)
		  
        val resourceMap:HashMap[String, Any] = new HashMap()
        resourceMap(FeatureResource.ResourceStr_ItemFeature) = featureStruct
        
        Logger.info("Saved item features and feature map")
        
        new FeatureResource(true, Some(resourceMap), resourceIden)
	}
	
	/**
	 * generate show time features.
	 */
	def generateFeatureVector(
	        channelSet: Set[String],
	        sourceMap:scala.collection.Map[String,Int]): Vector = {
	    var featureVect:Set[(Int, Double)] = Set()
	    
	    channelSet.foreach{channelId => 
	        if(sourceMap.isDefinedAt(channelId)){
	        	val featureUnit = (sourceMap(channelId), 1.0)
	        	featureVect = featureVect + featureUnit
	        }
	    }
	    
	    Vectors.sparse(sourceMap.size, featureVect.toSeq)
	}
	
	
}