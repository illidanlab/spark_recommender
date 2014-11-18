package com.samsung.vddil.recsys.utils

import org.apache.spark.rdd.RDD

object RDDOperations {
    def saveRddAsBlocks(
            dataset: RDD[(String, ((Int, Double), (String, Long)))], 
            blockNum: Int, 
            blockFilePath: String):Array[String] =  {
    	
        
        
        val partitionedRDD = dataset.repartition(blockNum)
        val blockSize      = partitionedRDD.partitions.size
        val blockFiles     = new Array[String](blockSize) 
        
        
        for ((dataBlock, blockId) <- dataset.partitions.zipWithIndex){
            val idx = dataBlock.index
            val blockRDD = partitionedRDD.mapPartitionsWithIndex(
                    (ind, x) => if (ind == idx) x else Iterator(), true)
            
            val blockFile = blockFilePath + "_" + idx 
            //be sure to check if file exists or not. 
            
            blockRDD.saveAsObjectFile(blockFile)
            blockFiles(blockId) = blockFile
        }
        
        blockFiles
    }
}