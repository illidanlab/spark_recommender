package com.samsung.vddil.recsys.data

import com.samsung.vddil.recsys.feature.FeatureStruct
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.ResourceStruct
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext._
import com.samsung.vddil.recsys.linalg.SparseVector

/**
 * This is the data structure for data
 */
trait DataStruct extends ResourceStruct{

}

/**
 * A data structure for combined data sets. 
 * 
 * @param resourceStr the unique identity of the dataset 
 *        resourcePrefix + data hash string
 * @param resourceLoc the location the dataset is stored. 
 * @param userList a list of users
 * @param itemList a list of items 
 * @param userMap mapping from user ID string to integer
 * @param itemMap mapping from item ID string to integer
 * @param dates the dates from which the combined dataset is generated. 
 */
class CombinedDataSet(
        val resourceStr: String,
        val resourceLoc: String,
        val userListLoc: String,
        val itemListLoc: String,
        val userMapLoc: String,
        val itemMapLoc: String,
        val userNum: Int,
        val itemNum: Int,
        val recordNum: Long,
        val dates: Array[String]
        ) extends DataStruct{
    
    val resourcePrefix = CombinedDataSet.resourcePrefix
    
    /**
     * Create a subset of combined data. 
     */
    def createSubset(
            resourceStr:String, 
            resourceLoc:String 
            ): CombinedDataSet = {
        new CombinedDataSet(
	        resourceStr: String, resourceLoc: String,
	        userListLoc: String, itemListLoc: String,
	        userMapLoc: String,  itemMapLoc: String,
	        userNum: Int, itemNum: Int,
	        -1, dates: Array[String]
        )
    }
    
    def getDataRDD(minPartitionNum:Option[Int] = None): RDD[(Int, Int, Double)] = {
        if (minPartitionNum.isDefined)
            Pipeline.instance.get.sc.textFile(resourceLoc, minPartitionNum.get).map{line => 
            	val splits = line.split(",")
            	val userId = line(0).toInt
            	val itemId = line(1).toInt
            	val rating    = line(2).toDouble
            	(userId, itemId, rating)
        	}
        	
        else
            Pipeline.instance.get.sc.textFile(resourceLoc).map{line =>
            	val splits = line.split(",")
            	val userId = line(0).toInt
            	val itemId = line(1).toInt
            	val rating    = line(2).toDouble
            	(userId, itemId, rating)
        	}
    }
    
    /**
     * Save an RDD to current resource location. 
     */
    def saveDataRDD(dataRDD: RDD[(Int, Int, Double)]): Unit = {
        CombinedDataSet.saveDataRDD(dataRDD, resourceLoc)
    }
    
    def getUserList(minPartitionNum:Option[Int] = None): RDD[String] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.textFile(userListLoc, minPartitionNum.get)
        else
            Pipeline.instance.get.sc.textFile(userListLoc)
    }
    
    def getItemList(minPartitionNum:Option[Int] = None): RDD[String] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.textFile(itemListLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.textFile(itemListLoc)
    }
    
    def getUserMap(minPartitionNum:Option[Int] = None): RDD[(String, Int)] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.objectFile[(String, Int)](userMapLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.objectFile[(String, Int)](userMapLoc)
    }
    
    def getItemMap(minPartitionNum:Option[Int] = None): RDD[(String, Int)] = {
        if (minPartitionNum.isDefined)
        	Pipeline.instance.get.sc.objectFile[(String, Int)](itemMapLoc, minPartitionNum.get)
        else
        	Pipeline.instance.get.sc.objectFile[(String, Int)](itemMapLoc)
    }
}

object CombinedDataSet{
    val resourcePrefix = "CombinedData"
        
    def saveDataRDD(dataRDD: RDD[(Int, Int, Double)], resourceLoc:String): Unit = {
        dataRDD.map{line =>
            line._1 + "," + line._2 + "," + line._3
        }.saveAsTextFile(resourceLoc)
    }    
}


trait AssembledDataSet extends DataStruct{
    val resourceStr: String
    val resourceLoc: String 
    val userFeatureOrder: List[FeatureStruct]
    val itemFeatureOrder: List[FeatureStruct]
    val combData: CombinedDataSet
    var size:Long
    var dimension:Int
    
    val resourcePrefix = "AssembledData"
    
    /** Splitting information e.g, ("test_001"->DataSplitting, "test_002"->DataSplitting)  */
    val splittings:HashMap[String, DataSplit] = HashMap()
    
    /** Set a splitting of this data set. */
    def putSplit(splitName: String, 
            tr: AssembledDataSet, 
            te: AssembledDataSet, 
            va: AssembledDataSet):Unit = {
        
        putSplit(splitName, DataSplit(tr, te, va))
    }
    
    /**  Set a splitting of this data set.*/
    def putSplit(splitName: String, split: DataSplit ):Unit = {
        if(splittings.isDefinedAt(splitName)){
            Logger.warn(s"The splitting $splitName already exists for $resourceIden, will be overwritten." )
        }
        
        splittings(splitName) = split
    }
    
    /** Get a splitting from this data set. */
    def getSplit(splitName: String):Option[DataSplit] = {
        if (splittings.isDefinedAt(splitName))
        	Some(splittings(splitName))
        else
            None
    }
    
    /** 
     *  Get data LabelPoint data structures. 
     *  The implementation can be different for online and offline photos. 
     */
    def getLabelPointRDD():RDD[LabeledPoint]
    
    //def createSplitStruct(resourceIden:String, resourceLoc:String): AssembledDataSet
}




/**
 * This is the data used to store an assembled feature, which includes 
 * features and their orders used in assembling them.  
 * 
 * @param resourceStr Resource string
 * @param resourceloc Resource location
 * @param userFeatureOrder Order of user feature, each element is the resource identity of a specific user feature
 * @param itemFeatureOrder Order of item feature, each element is the resource identity of a specific user feature
 * @param size the size of current dataset (-1 means does not check).  
 * 
 */
case class AssembledOfflineDataSet(
    val resourceStr: String,
    val resourceLoc: String, 
    val userFeatureOrder: List[FeatureStruct],
    val itemFeatureOrder: List[FeatureStruct],
    val combData: CombinedDataSet,
    var size: Long = -1,
    var dimension: Int = -1
    ) extends DataStruct with AssembledDataSet{
    
    def createSplitStruct(resourceIden:String, resourceLoc:String): AssembledDataSet = {
        new AssembledOfflineDataSet(resourceIden, resourceLoc, userFeatureOrder, itemFeatureOrder, combData)
    }
    
    /* Read offline file and parse data LabelPoint data structures. */
    def getLabelPointRDD():RDD[LabeledPoint] = {
        Pipeline.instance.get.sc.objectFile[(Int, Int, Vector, Double)](resourceLoc).
        map{tuple =>
            val rating:Double = tuple._4
            val feature:Vector = tuple._3
            LabeledPoint(rating, feature.toMLLib)
        }
    }
}

/**
 * In this online data set, the data is not cached in hard desk.  
 */
case class AssembledOnlineDataSet(
    val resourceStr: String,
    val userFeatureOrder: List[FeatureStruct],
    val itemFeatureOrder: List[FeatureStruct],
    val combData: CombinedDataSet,
    var size: Long = -1,
    var dimension: Int = -1
		) extends DataStruct with AssembledDataSet{
    
    val resourceLoc: String = null; 
    
    def createSplitStruct(
            resourceIden:String,  
            splitCombData: CombinedDataSet): AssembledDataSet = {
        new AssembledOnlineDataSet(resourceIden, userFeatureOrder, itemFeatureOrder, combData, size)
    }
    
    /** Read offline file and parse data LabelPoint data structures. */
    def getLabelPointRDD():RDD[LabeledPoint] = {
        //construct features 
        val watchTimeData = combData.getDataRDD()
        
        val userIdSet = combData.getItemMap().map{line => line._2}
        val itemIdSet = combData.getItemMap().map{line => line._2}
        
        val userFeaturesRDD = DataAssemble.reassembleFeatures(userIdSet, userFeatureOrder)
        val itemFeaturesRDD = DataAssemble.reassembleFeatures(itemIdSet, itemFeatureOrder)
        
        val joinedItemFeatures = 
              watchTimeData.map{x => 
                  (x._2, (x._1, x._3))
              }.join(
                   itemFeaturesRDD
              ).map{y => //(item, ((user, rating), IF))
                   val userID:Int = y._2._1._1
                   val itemID:Int = y._1
                   val itemFeature:Vector = y._2._2
                   val rating:Double = y._2._1._2
                   (userID, (itemID, itemFeature, rating))
              }
              
        val joinedUserItemFeatures = 
          	joinedItemFeatures.join(userFeaturesRDD
          	).map {x=> //(user, ((item, IF, rating), UF))
                //(user, item, UF, IF, rating)
                val userID = x._1
                val itemID = x._2._1._1
                val userFeature:SparseVector = x._2._2.toSparse
                val itemFeature:SparseVector = x._2._1._2.toSparse
                val feature = userFeature ++ itemFeature  
                val rating:Double = x._2._1._3
                //(userID, itemID, features, rating)
                LabeledPoint(rating, feature.toMLLib)
            }         
        
        joinedUserItemFeatures
    }
}


/**
 * Stores the splits of an AssembledDataset
 */
case class DataSplit(
        training:AssembledDataSet,
        testing:AssembledDataSet,
        validation:AssembledDataSet
     )

