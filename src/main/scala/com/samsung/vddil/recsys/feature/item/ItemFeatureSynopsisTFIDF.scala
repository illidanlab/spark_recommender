package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.SparseVector
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import scala.collection.mutable.HashMap
import scala.math.log
import scala.io.Source

/*
 * Item Feature: extract TFIDF numerical features from synopsis
 */
object ItemFeatureSynopsisTFIDF extends FeatureProcessingUnit with
ItemFeatureExtractor {

  val ItemIdInd = 1
  val ItemDescInd = 4
  val FeatSepChar = '|'
  val stopWords:Set[String] = {
    val fileLoc = "/stopwords.txt"
    val inputStream = getClass().getResourceAsStream(fileLoc)
    Source.fromInputStream(inputStream).mkString.split('\n').map(_.trim).toSet
  }


  def getItemsText(items:Set[String], featureSources:List[String], 
    sc:SparkContext):RDD[(String,String)] = {
    //broadcast item set
    val bItemsSet = sc.broadcast(items)
    
    //get passed items description
    val itemText:RDD[(String, String)] = featureSources.map{fileName =>
      val currItemText:RDD[(String, String)] = sc.textFile(fileName).map{line =>
        val fields = line.split(FeatSepChar)
        //get item id
        val itemId = fields(ItemIdInd)
        val text = if (fields.length > ItemDescInd) fields(ItemDescInd) else ""
        (itemId, text)
      }
      
      //remove empty text and item which are not presented in itemIdMap
      val filtCurrItemText:RDD[(String, String)] = currItemText.filter(x =>
        x._2.length > 0).filter(x => bItemsSet.value(x._1))
      filtCurrItemText
    }.reduce{ (a,b) =>
      //take union of RDDs
      a.union(b)
    }
    itemText.reduceByKey{(a, b) => a + " " + b}
  }


  def getItemTerms(itemsText:RDD[(String, String)], minTermLen:Int
    ):RDD[(String, List[String])] = {
    //tokenize item texts and apply some filters
    val itemTerms:RDD[(String, List[String])] = itemsText.map{itemText =>
      val itemId:String = itemText._1
      val text:String = itemText._2
      val tokens:List[String] = text.split("""\b""").filter{token =>
        //get only non-empty and starting with word
        val isWordRegex = """^\w+""".r
        (isWordRegex findFirstIn token).nonEmpty
      }.map{
        //convert to lower-case
        _.toLowerCase
      }.filter{
        //length of words is more than threshold
        _.length > minTermLen 
      }.filterNot{
        //remove stopwords
        stopWords(_)
      }.toList
      (itemId, tokens)
    }.filter{
      //consider only those items who have non-empty tokens
      _._2.length > 0
    }
    itemTerms
  }
      

  def getTermCounts(itemTerms:RDD[(String, List[String])], 
    topTerms:Array[String], sc:SparkContext):RDD[(String, SparseVector)] = {
    
    //broadcast top terms to each partition
    val bTopTerms = sc.broadcast(topTerms) 
      
    //get top term counts per item
    val itemTermCounts:RDD[(String, SparseVector)] = itemTerms.map{x =>
      val item = x._1
      val termsList = x._2
      val topTermCounts =
      bTopTerms.value.map{x =>
        termsList.count(_ == x).toDouble
      }
      (item, Vectors.sparse(topTermCounts))
    }

    itemTermCounts
  }


  def extractFeature(items:Set[String], featureSources:List[String],
    featureParams:HashMap[String, String], featureMapFileName:String, 
    sc:SparkContext): RDD[(String, SparseVector)] = {
    
    //get default parameters
    val N:Int = featureParams.getOrElse("N",  "500").toInt
	  val MinTermLen:Int = featureParams.getOrElse("MINTERMLEN", "2").toInt

    val tfIdfs:RDD[(String, Double)] = sc.textFile(featureMapFileName).map{line =>
      val fields = line.split(',')
      val term = fields(0)
      val score = fields(1).toDouble
      (term, score)
    }

    //get top N tf-idf terms sorted by decreasing score
    val sortedTerms = tfIdfs.collect.sortBy(-_._2) 
    val topTerms = sortedTerms.slice(0, N).map(_._1)
    
    //broadcast top terms to each partition
    val bTopTerms = sc.broadcast(topTerms) 
    
    //broadcast item set
    val bItemsSet = sc.broadcast(items)

    //get passed items description
    val itemsText:RDD[(String, String)] = getItemsText(items, featureSources, sc) 

    //tokenize the item texts and apply filters
    val itemTerms:RDD[(String, List[String])] = getItemTerms(itemsText,
      MinTermLen)
    
    //get top term counts or itemFeatures
    val itemFeatures:RDD[(String, SparseVector)] = getTermCounts(itemTerms,
      topTerms, sc)

    itemFeatures
  }



	def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
    
    //get spark context
    val sc = jobInfo.sc

    // 1. Complete default parameters
    val N:Int = featureParams.getOrElse("N",  "500").toInt
	  val MinTermLen:Int = featureParams.getOrElse("MINTERMLEN", "2").toInt

    // 2. Generate resource identity using resouceIdentity()
    val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
    val resourceIden = resourceIdentity(featureParams, dataHashingStr)
    
    val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
                  "/" + resourceIden
    var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
                  "/" + resourceIden + "_Map"
    
    //broadcast item map to workers to workonly on items relevant to training
    val itemIdMap = jobInfo.jobStatus.itemIdMap
    val bItemIdMap = sc.broadcast(itemIdMap)
      
    // 3. Feature generation algorithms (HDFS operations)
    var fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/program_desc*" 

    val featureSources:List[String] = jobInfo.trainDates.map{trainDate =>
      jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/program_desc*"
    }.toList
   
    //get items and its description
    val itemsText:RDD[(String, String)] = getItemsText(itemIdMap.keys.toSet,
      featureSources, sc)
  
    //tokenize the item texts and apply filters
    val itemTerms:RDD[(String, List[String])] = getItemTerms(itemsText,
      MinTermLen)

    //number of items
    val numItems = itemTerms.map(_._1).distinct.count.toDouble
    
    //terms count
    val termFreq:RDD[(String, Int)] = itemTerms.flatMap{_._2 map((_,1))}.reduceByKey(_+_)

    //terms - document frequency, i.e. no. of documents the term occurs
    val docFreq:RDD[(String, Double)] = itemTerms.flatMap{x =>
                                                      val itemId = x._1
                                                      val itemTerms = x._2
                                                      itemTerms.map((_, itemId))
                                                    }.groupByKey.map{x =>
                                                      val term = x._1
                                                      val numTermDocs =
                                                        x._2.toList.length.toDouble
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
    val itemTermCounts:RDD[(String, SparseVector)] = getTermCounts(itemTerms,
      topTerms, sc)
    //replace string id with int id for items
    val subItemTermCounts:RDD[(Int, SparseVector)] =
      itemTermCounts.map{itemTermCount =>
      val item:Int = bItemIdMap.value(itemTermCount._1)
      val feature = itemTermCount._2
      (item, feature)
    }

    //save these termcounts as item features
    if (jobInfo.outputResource(featureFileName)) {
      Logger.logger.info("Dumping feature resource: " + featureFileName)
      subItemTermCounts.saveAsObjectFile(featureFileName)
    }
    
    //save the terms with score
    if (jobInfo.outputResource(featureMapFileName)){
      Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
      tfIdfs.map {x =>
        val term = x._1
        val score = x._2
        term + "," + score
      }.saveAsTextFile(featureMapFileName)
    }
    
    val featureStruct:ItemFeatureStruct = 
        new ItemFeatureStruct(IdenPrefix, resourceIden, featureFileName, featureMapFileName)

    // 4. Generate and return a FeatureResource that includes all resources.  
		val resourceMap:HashMap[String, Any] = new HashMap()
    resourceMap(FeatureResource.ResourceStr_ItemFeature) = featureStruct
    
    Logger.info("Saved item features and feature map")

    new FeatureResource(true, Some(resourceMap), resourceIden)
	}
	
	val IdenPrefix:String = "ItemFeatureSynTFIDF"
   
}
