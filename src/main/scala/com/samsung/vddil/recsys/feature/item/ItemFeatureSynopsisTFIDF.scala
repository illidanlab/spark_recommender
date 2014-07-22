package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.SparseVector
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.Logger
import com.samsung.vddil.recsys.utils.HashString
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import scala.collection.mutable.HashMap
import scala.math.log

/*
 * Item Feature: extract TFIDF numerical features from synopsis
 */
object ItemFeatureSynopsisTFIDF extends FeatureProcessingUnit {

  val ItemIdInd = 1
  val ItemDescInd = 4

	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
		
    Logger.logger.error("%s has not been implmented.".format(getClass.getName()))
    
    //get spark context
    val sc = jobInfo.sc

    // 1. Complete default parameters
    val N:Int = featureParams.getOrElse("N",  "50").toInt
		
    // 2. Generate resource identity using resouceIdentity()
    val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
    val resourceIden = resourceIdentity(featureParams, dataHashingStr)
    
    val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
                  "/" + resourceIden
    var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
                  "/" + resourceIden + "_Map"
        
  
    // 3. Feature generation algorithms (HDFS operations)
    var fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/program_desc*" 
    val arrProgramTermsRDD:Array[RDD[(String, List[String])]] = jobInfo.trainDates.map {trainDate =>
                                            val fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) +
                                                            trainDate + "/program_desc*"
                                            sc.textFile(fileName).map {line =>
                                              val fields = line.split("|")
                                              val itemId = fields(ItemIdInd)
                                              val itemDesc = fields(ItemDescInd)
                                              //get terms from descriptioni
                                              //split description on word
                                              //boundary
                                              val terms = itemDesc.split("""\b""").filter(desc => {
                                                val isWordRegex = """^\w+""".r
                                                (isWordRegex findFirstIn
                                                  desc).nonEmpty
                                              }).toList
                                              (itemId, terms)
                                            }
                                          }
    //item id and terms
    val allProgramTerms:RDD[(String, List[String])] = arrProgramTermsRDD.reduce{(a,b) => 
                                            a.union(b)
                                          }.reduceByKey{(a,b) =>
                                            //combine two terms list for same
                                            //program
                                            a ++ b 
                                         }
    
    //number of items
    val numItems = allProgramTerms.map(_._1).distinct.count.toDouble
    
    //terms count
    val termFreq:RDD[(String, Int)] = allProgramTerms.flatMap{_._2 map((_,1))}.reduceByKey(_+_)

    //terms - document frequency, i.e. no. of documents term occurs
    val docFreq:RDD[(String, Int)] = allProgramTerms.flatMap{x =>
                                                      val itemId = x._1
                                                      val itemTerms = x._2
                                                      itemTerms.map((_, itemId))
                                                    }.groupByKey.map{x =>
                                                      val term = x._1
                                                      val numTermDocs =
                                                        x._2.toList.length
                                                      (term, numTermDocs)
                                                    }
 
    //tf-idf score of terms
    val tfIdfs:RDD[(String, Double)] = termFreq.join(docFreq).map{x =>
                                                               val term = x._1
                                                               val freq =
                                                                 x._2._1
                                                               val docFreq =
                                                                 x._2._2
                                                               (term, freq*log(numItems/docFreq)) 
                                                             }
 
    //get top N tf-idf terms sorted by decreasing score
    val sortedTerms = tfIdfs.collect.sortBy(-_._2) 
    val topTerms = sortedTerms.slice(0, N).map(_._1)

    //broadcast top terms to each partition
    val bTopTerms = sc.broadcast(topTerms) 

    //get top term counts per item
    val itemTermCounts:RDD[(String, SparseVector)] = allProgramTerms.map{x =>
                                          val item = x._1
                                          val termsList = x._2
                                          val topTermCounts =
                                          bTopTerms.value.map{x =>
                                            termsList.count(_ == x).toDouble
                                          }
                                          (item, Vectors.sparse(topTermCounts))
                                        }

    //save these termcounts as item features
    if (jobInfo.outputResource(featureFileName)) {
      //why ? Logger.logger not Logger.info
      Logger.logger.info("Dumping feature resource: " + featureFileName)
      itemTermCounts.saveAsObjectFile(featureFileName)
    }

    //save the terms with score
    if (jobInfo.outputResource(featureMapFileName)){
      Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
      tfIdfs.saveAsTextFile(featureMapFileName)
    }


    // 4. Generate and return a FeatureResource that includes all resources.  
		FeatureResource.fail
	}
	
	val IdenPrefix:String = "ItemFeatureSynTFIDF"
   
}
