package com.samsung.vddil.recsys.model

import scala.reflect._
import scala.collection.mutable.HashMap


import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.objenesis.strategy.StdInstantiatorStrategy
import org.apache.spark.mllib.linalg.{Vector => SV}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.mllib.optimization.FactorizationMachineRegressionModel
import org.apache.spark.mllib.optimization.CustomizedModel
import com.samsung.vddil.recsys.Pipeline
import com.samsung.vddil.recsys.linalg.Vector


/**
 * This is a trait for model. 
 * 
 * The necessary fields of a model 
 * 
 * @param modelName       the model name (least squares, or so)
 * @param resourceStr     the resource key for this model  
 * @param resourceLoc     the location in HDFS where the model will be saved.
 * @param modelParams the parameters of model
 */
trait ModelStruct extends Serializable{
	var modelName:String //IdenPrefix 
	var resourceStr:String //or resourceIden
	var modelParams:HashMap[String, String]
	var performance:HashMap[String, Double] //each key corresponds to one type of performance
	var learnDataResourceStr:String
	/**
	 * Predicts the result 
	 */
	def predict(testData: Vector): Double
}

object ModelStruct{
    val PerformanceTestMSE = "testMSE"
    val PerformanceTrainMSE = "trainMSE"
}

/**
 * An implementation of serializable model 
 */
trait SerializableModel [M <: Serializable ] extends ModelStruct{
    var model:M
	var modelFileName:String
    var performance:HashMap[String, Double] = new HashMap() 
    def ev: ClassTag[_] // record runtime. 
    /**
     * Serialize the model using Kyro serializer and save the model in a specified 
     * location [[com.samsung.vddil.recsys.model.SerializableModel.modelFileName]]. 
     */
    def saveModel() 
	
    /**
     * Deserialize the model from a file and load it from a specific location
     * [[com.samsung.vddil.recsys.model.SerializableModel.modelFileName]] 
     */
	def loadModel() 
}

/**
 * Allows a partial model, e.g., a partially completed model with a specific item feature. 
 * The computed partial feature then only requires a user feature, before it computes the 
 * prediction. 
 * 
 *  Though there are two types of partial functions provided, the most useful one is 
 *  applyItemFeature, where the item feature vector is enclosed.  
 */
trait PartializableModel extends ModelStruct{
    
    def modelFileName:String
    
    /**
     * Apply the item feature first to form a partial model.  
     * 
     * @itemFeature the item feature 
     * @return a function that predicts result given user feature
     */
    def applyItemFeature(itemFeature: Vector): Vector => Double = {
        def partialModel(userFeature: Vector):Double = this.predict(userFeature ++ itemFeature)
        partialModel
    }
    
    /**
     * Apply the item feature first to form a partial model.  
     * 
     * @itemFeature the user feature 
     * @return a function that predicts result given item feature 
     */
    def applyUserFeature(userFeature: Vector): Vector => Double = {
        def partialModel(itemFeature: Vector):Double = this.predict(userFeature ++ itemFeature)
        partialModel
    }
}




/**
 * The data structure for generalized linear models
 */
case class GeneralizedLinearModelStruct(
		    var modelName:String, 
		    var resourceStr:String, 
		    override var learnDataResourceStr:String, 
		    override var modelFileName:String,
		    var modelParams:HashMap[String, String] = new HashMap(), 
		    override var model:GeneralizedLinearModel
	    )(implicit val ev: ClassTag[GeneralizedLinearModel]) 
	      extends SerializableModel[GeneralizedLinearModel] with PartializableModel{
    
    def predict(testData: Vector) = model.predict(testData.toMLLib)
    
    override def saveModel() = {
        val out = Pipeline.instance.get.fs.create(new Path(modelFileName))
        
        val ser2 = Pipeline.instance.get.kryo.serializeStream(out).writeObject(model)
        
        ser2.close()
        out.close()
	}
    
    override def loadModel() = {
        val in = Pipeline.instance.get.fs.open(new Path(this.modelFileName))
                
        this.model = Pipeline.instance.get.kryo.deserializeStream(in).readObject[GeneralizedLinearModel]()

        in.close()
    }
}

/**
 * The data structure for customized model
 */
case class CustomizedModelStruct[M >: Null <: CustomizedModel](
        	var modelName:String, 
        	var resourceStr:String, 
        	override var learnDataResourceStr:String, 
        	override var modelFileName:String,
			var modelParams:HashMap[String, String] = new HashMap(), 
			override var model:M
        )(implicit val ev: ClassTag[M]) extends SerializableModel[M] with PartializableModel{
    
    def this(modelName:String, 
        	resourceStr:String, 
        	learnDataResourceStr:String, 
        	modelFileName:String,
			modelParams:HashMap[String, String])(implicit mf: Manifest[M])
		= this(modelName, resourceStr, learnDataResourceStr, modelFileName, modelParams, null)
		
	if (this.model == null){
	    this.loadModel()
	}
		
    def predict(testData: Vector) = model.predict(testData.toMLLib)
    
    override def saveModel() = {
        val out = Pipeline.instance.get.fs.create(new Path(modelFileName))
        
        val ser2 = Pipeline.instance.get.kryo.serializeStream(out).writeObject(model)
        ser2.close()

        out.close()
	}
    
    override def loadModel() = {
        val in = Pipeline.instance.get.fs.open(new Path(this.modelFileName))
                
        this.model = Pipeline.instance.get.kryo.deserializeStream(in).readObject[M]()

        in.close()
    }
}

