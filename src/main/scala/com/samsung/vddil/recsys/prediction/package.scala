package com.samsung.vddil.recsys

/**
 * This package includes components for computing recommendation results.  
 * 
 */
import org.apache.spark.rdd.RDD
import com.samsung.vddil.recsys.utils.Logger
import com.samsung.vddil.recsys.model.ModelStruct
import com.samsung.vddil.recsys.model.PartializableModel
import com.samsung.vddil.recsys.linalg.Vector
import org.apache.spark.SparkContext

package object prediction {
    
    /**
     * Computes the prediction given features and models. 
     * 
     * @param model a model struct
     * @param userFeaturesRDD user features 
     * @param itemFeaturesRDD
     * @param outputResource determines if a resource is ready to be output
     * @param PredBlockFiles block prediction files for partial model
     * @param ItemUserFeatFile entire prediction files for regular model 
     * @param sc SparkContext instance
     * @param partitionNum parallelism 
     * @param partialModelBatchNum the number of batches for partial model. 
     * 
     * Type parameters:
     * @param UserIDType typically integer or string
     * @param ItemIDType typically integer or string
     */
	def computePrediction[UserIDType, ItemIDType] (
            	model:ModelStruct,
            	userFeaturesRDD: RDD[(UserIDType, Vector)],
            	itemFeaturesRDD: RDD[(ItemIDType, Vector)],
            	outputResource: String => Boolean,
            	PredBlockFiles:String,
            	ItemUserFeatFile:String,
            	sc: SparkContext,
            	partitionNum:Int,
            	partialModelBatchNum:Int = 10
    		):RDD[(UserIDType, (ItemIDType, Double))] = {
        
        if (model.isInstanceOf[PartializableModel]){
        //if the model is a partializable model, then we use partial model 
        //and apply the models in batch. 
	        computePredictionWithPartialModel[UserIDType, ItemIDType] (
            	model.asInstanceOf[PartializableModel],
            	userFeaturesRDD, itemFeaturesRDD,
            	partialModelBatchNum, PredBlockFiles,
            	outputResource, sc, partitionNum)
	    }else{
	        //compute using traditional Cartisan production. 
	        computePredictionWithRegularModel[UserIDType, ItemIDType](
            	model, userFeaturesRDD, itemFeaturesRDD, 
            	ItemUserFeatFile,
            	outputResource, sc, partitionNum)
	    }

    }
    
	/**
	 * Computes prediction using a partial model
	 */
    def computePredictionWithPartialModel[UserIDType, ItemIDType] (
            	model: PartializableModel,
            	userFeaturesRDD: RDD[(UserIDType, Vector)],
            	itemFeaturesRDD: RDD[(ItemIDType, Vector)],
            	partialModelBatchNum:Int,
            	PredBlockFiles: String,
            	outputResource: String => Boolean,
            	sc:SparkContext,
            	partitionNum:Int
            	
            ):RDD[(UserIDType, (ItemIDType, Double))] = {
        
		Logger.info("Generating item enclosed partial models")
        var itemPartialModels = itemFeaturesRDD.map{x=>
            val itemId:ItemIDType = x._1
            val partialModel = model.applyItemFeature(x._2)
            (itemId, partialModel)
        }.coalesce(partialModelBatchNum)
        
        val predBlockSize = itemPartialModels.partitions.size
        val blockPredFiles = new Array[String](predBlockSize)
        Logger.info("Item enclosed partial models are divdided into " + predBlockSize + " blocks.")
        
        Logger.info("Preceed with partial models")
        for ((partialModelBlock, blockId) <- itemPartialModels.partitions.zipWithIndex){
            val idx = partialModelBlock.index
            val blockRdd = itemPartialModels.mapPartitionsWithIndex(
                    (ind, x) => if (ind == idx) x else Iterator(), true)
            
            //collect the models by block 
            var blockItemPartialModels = blockRdd.collect()
        	Logger.info("Broadcast block [ " + blockId + ":" + idx + "] with size:" + blockItemPartialModels.size)
        	
        	//block file location. 
        	blockPredFiles(idx) = PredBlockFiles + "_" + idx
        	
        	if (outputResource(blockPredFiles(idx))){
		        val bcItemPartialModels = sc.broadcast(blockItemPartialModels) 
		        //for each user compute the prediction for all items in this block. 
		        userFeaturesRDD.flatMap{x=>
		            val userId: UserIDType = x._1
		            val userFeature:Vector = x._2
		            
		            //itemPartialModelMap will be shipped to executors.  
		            bcItemPartialModels.value.map{x =>
		                val itemId:ItemIDType = x._1
		                (userId, (itemId, x._2(userFeature) ))
		            }
		        }.saveAsObjectFile(blockPredFiles(idx))
        	}
        }
        
        //load all predict blocks and aggregate. 
        Logger.info("Loading and aggregating " + predBlockSize + " blocks.")
        var aggregatedPredBlock:RDD[(UserIDType, (ItemIDType, Double))] = sc.emptyRDD[(UserIDType, (ItemIDType, Double))]
        for (idx <- 0 until predBlockSize){
            aggregatedPredBlock = aggregatedPredBlock ++ 
            		sc.objectFile[(UserIDType, (ItemIDType, Double))](blockPredFiles(idx))
        }
        aggregatedPredBlock.coalesce(partitionNum)
    }
    
    /**
     * Computes prediction using a regular model 
     *  
     */
    def computePredictionWithRegularModel[UserIDType, ItemIDType](
            	model:ModelStruct, 
            	userFeaturesRDD: RDD[(UserIDType, Vector)],
            	itemFeaturesRDD: RDD[(ItemIDType, Vector)],
            	ItemUserFeatFile:String,
            	outputResource: String => Boolean,
            	sc:SparkContext,
            	partitionNum:Int
    		):RDD[(UserIDType, (ItemIDType, Double))] = {
        //for each user get all possible user item features
	    Logger.info("Generating all possible user-item features")
	
	    if (outputResource(ItemUserFeatFile)){
	        val sampledUFIFRDD = userFeaturesRDD.cartesian(itemFeaturesRDD
	            ).map{ x=> //((userID, userFeature), (itemID, itemFeature))
	                val userID:UserIDType = x._1._1
	                val itemID:ItemIDType = x._2._1
	                val feature:Vector = x._1._2 ++ x._2._2
	                (userID, (itemID, feature))
	            }
	        //NOTE: by rearranging (userID, (itemID, feature)) we want to use
	        //      the partitioner by userID.
	        sampledUFIFRDD.coalesce(partitionNum).saveAsObjectFile(ItemUserFeatFile)
	    }
	    val userItemFeat = sc.objectFile[(UserIDType, (ItemIDType, Vector))](ItemUserFeatFile)
	    		
	    //for each user in test get prediction on all train items
	    userItemFeat.mapPartitions{iter =>                 
	              def pred: (Vector) => Double = model.predict
	              //(item, prediction)
	              iter.map( x => (x._1, (x._2._1, pred(x._2._2)))) 
	            }
    }
    

    
    
}