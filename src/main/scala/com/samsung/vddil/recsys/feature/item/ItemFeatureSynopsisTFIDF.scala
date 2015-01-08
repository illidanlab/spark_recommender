package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.Vector
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
import com.samsung.vddil.recsys.feature.process.FeaturePostProcess
import com.samsung.vddil.recsys.feature.process.FeaturePostProcessor
import com.samsung.vddil.recsys.feature.FeatureStruct

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

  def getItemsText(
          itemMap:RDD[(String, Int)], 
          featureSources:List[String], 
          sc:SparkContext):RDD[(String, String)] = {
      
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
      val filtCurrItemText:RDD[(String, String)] = 
          currItemText.filter(x => x._2.length > 0).
          join(itemMap).map{ line => //(itemIdStr, (itemText, itemIdInt))
          val itemIdStr:String = line._1
          val itemText :String = line._2._1
          (itemIdStr, itemText)
      }
      filtCurrItemText
    }.reduce{ (a,b) =>
      //take union of RDDs
      a.union(b)
    }
    itemText.reduceByKey{(a, b) => a + " " + b}
  }
  
  
  /**
   * @deprecated
   */
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

  /**
   * Tokenizes and turns the item description into a set of words.  
   */
  def getItemTerms(
          itemsText:RDD[(String, String)], 
          minTermLen:Int):RDD[(String, List[String])] = {
      
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
      
  /**
   * Given a tokenized words for each item, this method generates a 
   * vector, where each dimension corresponds to the tfidf value at  
   * the corresponding term in the input topTerms.
   * 
   * @param itemTerms 
   * @param topTerms represents the ordered keywords, where tfidf is computed.
   * @param sc SparkContext instance 
   * @deprecated
   */
  def getTermCounts(
          itemTerms:RDD[(String, List[String])], 
          topTerms:Array[String], 
          sc:SparkContext):RDD[(String, Vector)] = {
    
    //broadcast top terms to each partition
    val bTopTerms = sc.broadcast(topTerms) 
      
    //get top term counts per item
    val itemTermCounts:RDD[(String, Vector)] = itemTerms.map{x =>
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

  /**
   * Given a tokenized words for each item, this method generates a 
   * vector, where each dimension corresponds to the tfidf value at  
   * the corresponding term in the input topTerms.
   * 
   * @param itemTerms 
   * @param topTerms represents the ordered keywords, where tfidf is computed.
   * @param sc SparkContext instance 
   */
  def getTermCounts(
          itemTerms:RDD[(String, List[String])], 
          topTermsWithId:Map[String,(Int, Double)], 
          sc:SparkContext):RDD[(String, Vector)] = {
    
    //broadcast top terms to each partition
    val bTopTerms = sc.broadcast(topTermsWithId) 
    
    val numTopTerms = topTermsWithId.size
    //get top term counts per item
    val itemTermCounts:RDD[(String, Vector)] = itemTerms.map{x =>
      val item = x._1
      val termsList = x._2
      
      var featureVecPair = Map[Int, Double]()
      termsList.foreach{ term =>
          if(topTermsWithId.isDefinedAt(term)){
              val entry = topTermsWithId(term)
              val termIdx = entry._1
              // update 
              val value = featureVecPair.getOrElse(termIdx, 0.0)
        	  featureVecPair = featureVecPair.updated(termIdx, value + 1.0)
          }
      }
      val featureVec = Vectors.sparse(numTopTerms, featureVecPair.toList)
      (item, featureVec)
    }

    itemTermCounts
  }

  def getFeatureSources(dates:List[String], jobInfo:RecJob):List[String] = {
    dates.map{date =>
      jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + date + "/program_desc*"
    }.toList
  }
  

  def extractFeature(
          items:Set[String], featureSources:List[String],
          featureParams:HashMap[String, String], featureMapFileName:String,
          sc:SparkContext): RDD[(String, Vector)] = {
    
    //get default parameters
    val N:Int = featureParams.getOrElse("N",  "500").toInt
	val MinTermLen:Int = featureParams.getOrElse("MINTERMLEN", "2").toInt
    
	  
	val topTermsWithId = sc.textFile(featureMapFileName).map{line => 
        val fields        = line.split(',')
        val idx:Int       = fields(0).toInt
        val term:String   = fields(1)
        val value:Double  = fields(2).toDouble
        //(idx, (term, value))
        (term, (idx,  value))
    }.collectAsMap.toMap
    
    //broadcast item set
    val bItemsSet = sc.broadcast(items)

    //get passed items description
    val itemsText:RDD[(String, String)] = getItemsText(items, featureSources, sc) 

    //tokenize the item texts and apply filters
    val itemTerms:RDD[(String, List[String])] = getItemTerms(itemsText,
      MinTermLen)
    
    //get top term counts or itemFeatures
    val itemFeatures:RDD[(String, Vector)] = getTermCounts(itemTerms, topTermsWithId, sc)
    
    itemFeatures
  }



	def processFeature(
	        featureParams:HashMap[String, String], 
	        jobInfo:RecJob):FeatureResource = {
    
	  	val trainCombData = jobInfo.jobStatus.resourceLocation_CombinedData_train.get
	    
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
	    val itemIdMap = trainCombData.getItemMap().collectAsMap
	    val bItemIdMap = sc.broadcast(itemIdMap)
	    //val itemIdMap = trainCombData.getItemMap()
	                  
	    // 3. Feature generation algorithms (HDFS operations)
	    val featureSources:List[String] = getFeatureSources(
	        jobInfo.trainDates.toList, jobInfo)      
	   
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
	    //val topTerms = sortedTerms.slice(0, N).map(_._1)
	    
	    //associate an id for order purpose. 
	    val topTermsWithIdArray:Array[(Int, (String, Double))] = 
	        sortedTerms.slice(0, N).zipWithIndex.map{line =>
	        	val idx   = line._2
	        	val term  = line._1._1
	        	val value = line._1._2
	        	(idx, (term, value))
	    	}
	    
	    //save the terms with score
	    if (jobInfo.outputResource(featureMapFileName)){
	        Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)

	        val featureMapRdd = sc.parallelize(topTermsWithIdArray.map{line => 
	            val idx   = line._1
	            val term  = line._2._1
	            val score = line._2._2
	            (idx, term + "," + score) 
	        })
	        	        
	        FeatureStruct.saveText_featureMapRDD(featureMapRdd, featureMapFileName, jobInfo)
	    }
	    
	    val topTermsWithId = topTermsWithIdArray.map{line => 
	        val idx   = line._1
	        val term  = line._2._1
	        val value = line._2._2
	        (term, (idx, value))
	    }.toMap
	    
	    //get top term counts per item
	    val itemTermCounts:RDD[(String, Vector)] = getTermCounts(itemTerms, topTermsWithId, sc)
	      
	      
	    //replace string id with int id for items
	    val subItemTermCounts:RDD[(Int, Vector)] =
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
	    //Enable plain dump to check the features manually. 
	    //subItemTermCounts.saveAsTextFile(featureFileName + "_txtDump")

	    
	    val featureSize = subItemTermCounts.first._2.size;
	    
	    val featurePostProcessor:List[FeaturePostProcessor] = List()
	    val featureStruct:ItemFeatureStruct = 
	        new ItemFeatureStruct(
	                IdenPrefix, resourceIden, featureFileName, 
	                featureMapFileName, featureParams, featureSize, 
	                featureSize, featurePostProcessor, 
	                ItemFeatureSynopsisTFIDF, None)
	
	    // 4. Generate and return a FeatureResource that includes all resources.  
			val resourceMap:HashMap[String, Any] = new HashMap()
	    resourceMap(FeatureResource.ResourceStr_ItemFeature) = featureStruct
	    resourceMap(FeatureResource.ResourceStr_FeatureDim)  = featureSize
	        
	    Logger.info("Saved item features and feature map")
	
	    new FeatureResource(true, Some(resourceMap), resourceIden)
		}
		
		val IdenPrefix:String = "ItemFeatureSynTFIDF"
	   
	}
