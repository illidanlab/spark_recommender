package com.samsung.vddil.recsys.model

import scala.collection.mutable.HashMap
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import com.samsung.vddil.recsys.Pipeline
import org.apache.spark.mllib.optimization.FactorizationMachineRegressionModel
import org.apache.spark.mllib.optimization.CustomizedModel

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
	def predict(testData: org.apache.spark.mllib.linalg.Vector): Double
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
    
    /**
     * Serialize the model using Kyro serializer and save the model in a specified 
     * location [[com.samsung.vddil.recsys.model.SerializableModel.modelFileName]]. 
     */
    def saveModel(sc: SparkContext){
      	
		val out = Pipeline.instance.get.fs.create(new Path(modelFileName))

		val kyro:Kryo = new Kryo()
		kyro.writeObject(new Output(out), model)
		
		out.close()
	}
	
    /**
     * Deserialize the model from a file and load it from a specific location
     * [[com.samsung.vddil.recsys.model.SerializableModel.modelFileName]] 
     */
	def loadModel(sc: SparkContext)(implicit mf: ClassManifest[M]){ 
        
	    val in = Pipeline.instance.get.fs.open(new Path(modelFileName))
        
        val kryo:Kryo = new Kryo()
        this.model = kryo.readObject(new Input(in), mf.runtimeClass).asInstanceOf[M]
        
        in.close()
	}
}

/**
 * The data structure for generalized linear models
 */
case class GeneralizedLinearModelStruct(
		    var modelName:String, 
		    var resourceStr:String, 
		    override var learnDataResourceStr:String, 
		    var modelFileName:String,
		    var modelParams:HashMap[String, String] = new HashMap(), 
		    override var model:GeneralizedLinearModel
	    ) extends SerializableModel[GeneralizedLinearModel]{
    
    def predict(testData: org.apache.spark.mllib.linalg.Vector) = model.predict(testData)
}

/**
 * The data structure for customized model
 */
case class CustomizedModelStruct[M <: CustomizedModel](
        	var modelName:String, 
        	var resourceStr:String, 
        	override var learnDataResourceStr:String, 
        	var modelFileName:String,
			var modelParams:HashMap[String, String] = new HashMap(), 
			override var model:M
        ) extends SerializableModel[M]{
    
    def predict(testData: org.apache.spark.mllib.linalg.Vector) = model.predict(testData)
}

