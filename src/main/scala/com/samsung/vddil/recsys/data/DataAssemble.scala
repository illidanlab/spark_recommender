package com.samsung.vddil.recsys.data


import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.job.RecJobStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap

import com.samsung.vddil.recsys.utils.HashString

object DataAssemble {
	
	
	def flattenNestedTuple(x : ((String,String),String)): String = {
		List(x._1._1, x._1._2, x._2).mkString(",")
	}
	
	def flattenTuple2(x : (String, String)): String = {
        List(x._1, x._2).mkString(",")
    }
	
	
	/**
	 * return join of features of specified Ids
	 */
	def getCombinedFeatures(idSet: Set[String], usedFeatures: HashSet[String], 
                  featureResourceMap: HashMap[String, String], 
                  sc: SparkContext):RDD[(String, String)] = {
		//initialize list of RDD of format (id,features)
		var idFeatures:List[RDD[(String, String)]]  = List.empty
		//TODO: need to save feature order
		val usedFeaturesList = usedFeatures.toList
		//add all features RDD to list
		for (usedFeature <- usedFeaturesList) {
			idFeatures  = idFeatures :+ sc.textFile(featureResourceMap(usedFeature))
			  .map { line =>
			  	val fields = line.split(',')
				val id = fields(0)
				val features = fields.slice(1, fields.length).mkString(",")
				(id, features)
			    }
			  .filter(x => idSet.contains(x._1)) //id matches specified id
		}
		
		//join features in idFeatures
		//TODO: check this functionality
		var joinedFeatures = idFeatures.head
		
		if (idFeatures.length > 3) {
			var tempJoinFeature = idFeatures(0).join(idFeatures(1)).join(idFeatures(2))
            var temp2 = tempJoinFeature.map {x =>
                    (x._1, flattenNestedTuple(x._2))
                }
            idFeatures = idFeatures.updated(2, temp2)
		}
		
		//below for loop will join 3 features at a time and in the last index will 
		//store the concatenated RDD
		for (i <- 0 until idFeatures.length by 2) {
		    
			if (i + 2 < idFeatures.length) {
			    //join 3 at a time
			    var tempJoinFeature = idFeatures(i).join(idFeatures(i+1)).join(idFeatures(i+2))
			
			    var temp2 = tempJoinFeature.map {x =>
				    (x._1, flattenNestedTuple(x._2))
				}
			    idFeatures = idFeatures.updated(i+2, temp2)
			} else if (i + 1 < idFeatures.length) {
				//join last 2
				var tempJoinFeature = idFeatures(i).join(idFeatures(i+1))
				var temp2 = tempJoinFeature.map {x =>
                    (x._1, flattenTuple2(x._2))
                }
				idFeatures = idFeatures.updated(i+1, temp2)
			}
		}
		
		//idFeatures[length-1] contains all features concatenated
		idFeatures(idFeatures.length - 1)
	}
	
	
	
	/**
	 * will return intersection of ids for which fetures exist
	 * usedFeatures: features for which we want intersection of ids
	 * featureResourceMap: contains mapping of features to actual files
	 * sc: SparkContext
	 */
	def getIntersectIds(usedFeatures: HashSet[String], 
			      featureResourceMap: HashMap[String, String], 
			      sc: SparkContext):  Set[String] = {
	    val usedFeaturesList = usedFeatures.toList
	    
	    //get Ids from first feature
		var intersectIds = sc.textFile(featureResourceMap(usedFeaturesList.head))
		                     .map{line  => 
		                         line.split(',')(0) //assuming first field is always id
	                          }
	    
	    //do intersection with other features
	    for (usedFeature <- usedFeaturesList.tail) {
	    	intersectIds = intersectIds.intersection(
	    			sc.textFile(featureResourceMap(usedFeature))
	    			  .map{line  => line.split(',')(0)}
	    			)
	    }
	    
	    intersectIds.collect.toSet
	}
	
	
	/**
	 * will return only those features which satisfy minimum coverage criteria
	 * featureResourceMap: contains map of features and location
	 * minCoverage: minimum coverage i.e. no. of features found should be greater than this pc
	 * sc: spark context
	 * total: number of items or users
	 */
	def filterFeatures(featureResourceMap: HashMap[String, String], 
			minCoverage: Double, sc: SparkContext, total: Int) :HashSet[String] = {
		//set to keep keys of item feature having desired coverage
        var usedFeatures:HashSet[String] = new HashSet()
        
        //check each feature against minCoverage
	    featureResourceMap foreach {
            case (k, v) =>
                {
                    val numFeatures = sc.textFile(v).count
                    if ( (numFeatures.toDouble/total)  > minCoverage) {
                    	//coverage satisfy by feature add it to used set
                        usedFeatures += k
                    }
                    
                }
	    }
        usedFeatures
	}
	
	
	/*
	 *  Joining features 
	 */
	def assembleContinuousData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double ):String = {
		
	  
	    //1. inspect all available features
		//   drop features have low coverage (which significant reduces our training due to missing)
	    //   TODO: minUserFeatureCoverage and minItemFeatureCoverage from file. 
		
	  
		//get spark context
	    val sc = jobInfo.sc
	    
	    //get num of users
	    val numUsers = jobInfo.jobStatus.users.length
	    
	    //get num of items
	    val numItems = jobInfo.jobStatus.items.length
	    
	    
	    //set to keep keys of item feature having desired coverage
	    val usedItemFeature:HashSet[String] = filterFeatures(
	    		            jobInfo.jobStatus.resourceLocation_ItemFeature, 
	    		                                             minIFCoverage, 
	    		                                             sc, numItems)

	    //set to keep keys of user feature having desired coverage
	    val usedUserFeature:HashSet[String] = filterFeatures(
                            jobInfo.jobStatus.resourceLocation_UserFeature, 
                                                             minUFCoverage, 
                                                             sc, numItems)
		
		//4. generate ID string 
		val resourceStr = assembleContinuousDataIden(usedUserFeature, usedItemFeature)
	  
		//check if the regression data has already generated in jobInfo.jobStatus
		//  it is possible this combination has been used (and thus generated) by other classifiers. 
		//  in that case directly return resourceStr. 
		if (! jobInfo.jobStatus.resourceLocation_AggregateData_Continuous.isDefinedAt(resourceStr)){
		
			//2. perform an intersection on selected user features, generate <intersectUF>
		    val userIntersectIds = getIntersectIds(usedUserFeature, 
		    		        jobInfo.jobStatus.resourceLocation_UserFeature, sc)
	  
		    //parse eligible features and extract only those with ids present in userIntersectIds
		    val userFeaturesRDD =  getCombinedFeatures(userIntersectIds, 
            		    		usedUserFeature, 
            		    		jobInfo.jobStatus.resourceLocation_UserFeature, 
            		    		sc)
            		    		        
		    		        
			//3. perform an intersection on selected item features, generate <intersectIF>
		    val itemIntersectIds = getIntersectIds(usedItemFeature, 
                            jobInfo.jobStatus.resourceLocation_ItemFeature, sc)
                            
            //parse eligible features and extract only those with ids present in itemIntersectIds
            val itemFeaturesRDD =  getCombinedFeatures(itemIntersectIds, 
                                usedItemFeature, 
                                jobInfo.jobStatus.resourceLocation_ItemFeature, 
                                sc)
		  
			//5. perform a filtering on ( UserID, ItemID, rating) using <intersectUF> and <intersectIF>, 
			//   and generate <intersectTuple>
		  
		  
			 
			   
			//6. join features and <intersectTuple> and generate aggregated data (UF1 UF2 ... IF1 IF2 ... , feedback )
			val assembleFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobData) + "/" + resourceStr + "_all"
		
			// join features and store in assembleFileName
			
			
			//7. save resource to <jobInfo.jobStatus.resourceLocation_AggregateData_Continuous>
			jobInfo.jobStatus.resourceLocation_AggregateData_Continuous(resourceStr) = assembleFileName 
		}
		 
	    return resourceStr
	}
	
	def assembleContinuousDataIden(userFeature:HashSet[String], itemFeature:HashSet[String]):String = {
		return "ContAggData_" + HashString.generateHash(userFeature.toString) + "_"  + HashString.generateHash(itemFeature.toString) 
	}
	
	def assembleBinaryData(jobInfo:RecJob, minIFCoverage:Double, minUFCoverage:Double):String = {
	    //see assembleContinuousData
		return null
	}
	
	def assembleBinaryDataIden(userFeature:HashSet[String], itemFeature:HashSet[String]):String = {
		return "BinAggData_" + HashString.generateHash(userFeature.toString) + "_"  + HashString.generateHash(itemFeature.toString)
	}
}