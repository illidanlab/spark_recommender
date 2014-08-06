package com.samsung.vddil.recsys.feature.item

import com.samsung.vddil.recsys.feature.FeatureProcessingUnit
import com.samsung.vddil.recsys.feature.FeatureResource
import com.samsung.vddil.recsys.feature.ItemFeatureStruct
import com.samsung.vddil.recsys.job.RecJob
import com.samsung.vddil.recsys.linalg.SparseVector
import com.samsung.vddil.recsys.linalg.Vectors
import com.samsung.vddil.recsys.utils.HashString
import com.samsung.vddil.recsys.utils.Logger
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.Map
import scala.collection.mutable.HashMap

object ItemFeatureGenre  extends FeatureProcessingUnit with ItemFeatureExtractor {
  
    val ItemGenreInd = 2
    val ItemIdInd = 1
    val FeatSepChar = '|'
    val GenreIdInd = 1
    val GenreLangInd = 2
    val GenreDescInd = 3
    val GenreLangFilt = "en" //default language 
      
    val Param_GenreLang = "lang" 
 

  /**
   * get the genre for passed items
   * @param items set of items for which genre list needs to be retrieved
   * @param genre2Ind map of genre string to index
   * @param featureSources list of files from which feature will be extracted
   * @param sc SparkContext instance
   * @return RDD of item, genre pair
   */
  def getItemGenreList(items:Set[String], genre2Ind:Map[String, Int],
    featureSources:List[String], sc:SparkContext):RDD[(String, String)] = {
    val bItemsSet = sc.broadcast(items)
    val bGenre2Ind = sc.broadcast(genre2Ind)
    val itemGenreList:RDD[(String, String)] = featureSources.map{fileName =>
      val itemsGenre:RDD[(String, String)] = sc.textFile(fileName).map{line =>
        val fields = line.split(FeatSepChar)
        val item = fields(ItemIdInd)
        val genre = fields(ItemGenreInd)
        (item, genre)
      }.filter{itemGenre =>
        val item = itemGenre._1
        val genre = itemGenre._2
        bItemsSet.value(item) && bGenre2Ind.value.contains(genre)
      }
      itemsGenre
    }.reduce{(a,b) =>
      a.union(b)
    }.distinct
    itemGenreList
  }


  /**
   * from list of (item, genre) pairs create feature vectors for items
   * @param itemGenreList RDD of item, genre pair
   * @param genre2Ind map of genre string to index
   * @return RDD of item and its genre feature vector
   */
  def itemGenreListToFeature(itemGenreList:RDD[(String, String)], 
    genre2Ind:Map[String, Int]):RDD[(String, SparseVector)] = {
   
    val numGenres:Int = genre2Ind.size
    itemGenreList.groupByKey.map{x =>
      val itemId = x._1
     
      //save to sparse vector
      var featureVecPair = Seq[(Int, Double)]()
      for (genre <- x._2) {
        featureVecPair = featureVecPair :+ (genre2Ind(genre), 1.0)
      }
      val featureVec = Vectors.sparse(numGenres, featureVecPair)
      (itemId, featureVec)
    }
  }


  /**
   * extract features for passed items based on feature map
   * @param items set of passed item
   * @param featureSources list of files to extract feature
   * @param featureParams passed feature parameters at time of training
   * @param featureMapFileName contains mapping from genre to indices
   * @param sc spark context instance
   * @return paired RDD of item and its feature vector
   */
  def extractFeature(items:Set[String], featureSources:List[String],
    featureParams:HashMap[String, String], featureMapFileName:String, 
    sc:SparkContext): RDD[(String, SparseVector)] = {
    
    val genreInd2KeyDesc:RDD[(Int, String, String)] =
      sc.textFile(featureMapFileName).map{line =>
        val fields = line.split(',')
        val genreInd:Int = fields(0).toInt
        val genreKey:String = fields(1)
        val genreDesc:String = fields(2)
        (genreInd, genreKey, genreDesc)
      }
    
    val genre2Ind:Map[String, Int] = genreInd2KeyDesc.map{x =>
      val genreKey:String = x._2
      val genreInd:Int = x._1
      (genreKey, genreInd)
    }.collectAsMap

    val itemGenreList:RDD[(String, String)] = getItemGenreList(items, genre2Ind, 
      featureSources, sc)
    itemGenreListToFeature(itemGenreList, genre2Ind) 
  }



    def processFeature(featureParams:HashMap[String, String], jobInfo:RecJob):FeatureResource = {
    		
    	//get spark context
        val sc = jobInfo.sc
        
        // 1. Complete default parameters
        //  default parameter for genre: lang filtering. 
        var param_GenreLang:String = GenreLangFilt
        if(featureParams.isDefinedAt(Param_GenreLang)){
            param_GenreLang = featureParams(Param_GenreLang)
        }
          
        // 2. Generate resource identity using resouceIdentity()
        val dataHashingStr = HashString.generateOrderedArrayHash(jobInfo.trainDates)
        val resourceIden = resourceIdentity(featureParams, dataHashingStr)
        
        val featureFileName    = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden
        var featureMapFileName = jobInfo.resourceLoc(RecJob.ResourceLoc_JobFeature) + 
        							"/" + resourceIden + "_Map"
        
        // 3. Feature generation algorithms (HDFS operations)
        
        //get set of items
        val itemSet = jobInfo.jobStatus.items.toSet
        
        
        //get RDDs of genres only for param_GenreLang if exists
        var fileName = jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + jobInfo.trainDates(0) + "/genre*" 
        val genreSources = jobInfo.trainDates.map{trainDate =>
          jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/genre*" 
        }

        val genreMap:RDD[(String, String, String)] =
          genreSources.map{genreSource =>
            val currGenreMap:RDD[(String, String, String)] = sc.textFile(genreSource).map{line =>
              val fields = line.split('|')
              (fields(GenreIdInd), fields(GenreLangInd), fields(GenreDescInd))
            }.filter{genreTriplet =>
              val genreLang = genreTriplet._2
              genreLang == param_GenreLang
            }
            currGenreMap
          }.reduce{(a,b) =>
            a.union(b)
          }.distinct
        
        val genresWDesc = genreMap.collect
        val genreKeys = genresWDesc.map(x => x._1)
        val genreDesc = genresWDesc.map(x => x._3)
        val numGenres = genreKeys.length
        
        //create a map: index ->  (genreId or genreKey, description) 
        val genreInd2KeyDesc = ((0 until genreKeys.length) zip (genreKeys zip genreDesc)) toMap
        val genreInd2KeyDescRDD = sc.makeRDD(genreInd2KeyDesc.toList)
        
        //create reverse map for lookup 
        val genre2Ind = (genreKeys zip (0 until genreKeys.length)) toMap
        
        Logger.info("created genres to index map, numGenres: " + numGenres 
                    + " numItems: " + itemSet.size)
        
        //get RDDs of items itemGenreList: itemId, subgenre, Genre   
        //filter only those for which genre is already found
        val featureSources = jobInfo.trainDates.map{trainDate =>
          jobInfo.resourceLoc(RecJob.ResourceLoc_RoviHQ) + trainDate + "/program_genre*"
        }.toList
       
        val bItemsSet = sc.broadcast(itemSet)
        val bGenre2Ind = sc.broadcast(genre2Ind)
        val itemGenreList:RDD[(String, String)] = getItemGenreList(itemSet,
          genre2Ind, featureSources, sc)
        
       
        Logger.info("Created itemGenres list")
        
        val itemIdMap = jobInfo.jobStatus.itemIdMap
        val bItemMap = sc.broadcast(itemIdMap)
        //generate feature vector for each items    
        val itemFeature:RDD[(Int, SparseVector)] =
          itemGenreListToFeature(itemGenreList, genre2Ind).map{x =>
            val item:Int = bItemMap.value(x._1)
            val feature:SparseVector = x._2
            (item, feature)
          }

       
        Logger.info("created item genre feature vectors")
        
        //save item features as textfile
        if (jobInfo.outputResource(featureFileName)){
        	Logger.logger.info("Dumping feature resource: " + featureFileName)
        	itemFeature.saveAsObjectFile(featureFileName) //directly use object + serialization. 
        }
        
        //save genre mapping to indexes
        if (jobInfo.outputResource(featureMapFileName)){
        	Logger.logger.info("Dumping featureMap resource: " + featureMapFileName)
          genreInd2KeyDescRDD.map{genreInd2KeyDes => 
            val genreInd:Int = genreInd2KeyDes._1
            val genreKey:String = genreInd2KeyDes._2._1
            val genreDesc:String = genreInd2KeyDes._2._2
            genreInd + "," + genreKey + "," + genreDesc
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
    
    
    val IdenPrefix:String = "ItemFeatureGenre"
}
