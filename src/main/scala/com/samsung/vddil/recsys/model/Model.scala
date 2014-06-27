package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.regression.GeneralizedLinearModel


/**
 * This is a trait for model. 
 * 
 * The necessary fields of a model 
 * 
 * modelName:String       the model name (least squares, or so)
 * resourceStr:String     the resource key for this model  
 * resourceLoc:String     the location in HDFS where the model will be saved.
 * modelParams:HashMap[String, String]
 */
trait ModelStruct{
	var modelName:String //IdenPrefix 
	var resourceStr:String //or resourceIden
	var modelFileName:String
	var modelParams:HashMap[String, String]
	var performance:HashMap[String, Double] //each key corresponds to one type of performance
	
	/*
	 * Serialized the current model into a model file using SparkContext
	 */
	def saveModel(sc: SparkContext)
	
	/*
	 * Deserialize a model from HDFS. 
	 */
	def loadModel(sc: SparkContext)
	
}

object ModelStruct{
    val PerformanceTestMSE = "testMSE"
    val PerformanceTrainMSE = "trainMSE"
}

trait LinearModelStruct extends ModelStruct{
    
    var weights: Option[Vector] = None
	var intercept: Option[Double] = None
	var model:GeneralizedLinearModel
	
    var performance:HashMap[String, Double] = new HashMap() 
    
    def saveModel(sc: SparkContext){
      	weights = Some(model.weights)
      	intercept = Some(model.intercept)
      	
	    val hadoopConf = sc.hadoopConfiguration
		val fileSystem = FileSystem.get(hadoopConf)
		//create file on hdfs
		val out = fileSystem.create(new Path(modelFileName))

		//write weights if exists, note using Options hence first for each
		weights foreach { value =>
			value.toArray foreach {v => 
                                        out.writeChars(v.toString)  
                                        out.write(',')
			                        }
		}
		out.write('\n')
		
		//write intercept if exists
		intercept foreach {
			value => out.writeChars(value.toString)
		}
		
		//close file
		out.close()
	}
	
	def loadModel(sc: SparkContext){
	    //TODO: need to build Spark model from the intercept and weights. 
	    val hadoopConf = sc.hadoopConfiguration
        val fileSystem = FileSystem.get(hadoopConf)
        
        //open file on hdfs
        val in = fileSystem.open(new Path(modelFileName))
        
        //read weights
        val weightsStr =  in.readLine().trim().split(',')
        if (weightsStr.length > 0) {
        	weights = Some(Vectors.dense(weightsStr.map(_.toDouble)))
        }
        //read intercept
        val interceptStr = in.readLine().trim()
        if (interceptStr.length() > 0) {
        	intercept = Some(interceptStr.toDouble)
        }
        
        //close file
        in.close()
	}
}

case class LinearRegressionModelStruct(
		    var modelName:String, var resourceStr:String, var learnDataResourceStr:String, var modelFileName:String,
		    var modelParams:HashMap[String, String] = new HashMap(), var model:GeneralizedLinearModel
	    ) extends LinearModelStruct{
}

case class LinearClassificationModelStruct(
			var modelName:String, var resourceStr:String, var modelFileName:String,
			var modelParams:HashMap[String, String] = new HashMap(), var model:GeneralizedLinearModel
		) extends LinearModelStruct{
}



