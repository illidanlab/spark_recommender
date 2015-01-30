package org.apache.spark.mllib.optimization

import scala.collection.immutable.HashMap
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.RegressionModel
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{sum => brzSum, Axis}
import breeze.linalg.{norm => brzNorm, DenseMatrix => BDM, axpy => brzAxpy, Vector=> BV, DenseVector=> BDV}
import scala.util.Random
import com.samsung.vddil.recsys.linalg.ProxFunctions

class FeatureLearningFactorizationMachineRegressionModel (
        override val modelVector: Vector,
        override val param:HashMap[String, Any]
		) extends CustomizedModel(modelVector, param)
		with RegressionModel with Serializable
{

    //extract parameters 
    val latentDim:Int = 
        param(FeatureLearningFactorizationMachineRegressionModel.ParamLatentDimension).asInstanceOf[Int]
    val learnedFeatureSize:Int = 
        param(FeatureLearningFactorizationMachineRegressionModel.ParamLearnFeatureSize).asInstanceOf[Int]
    
    //devectorized model.
    val (pMatrix, wVector, vMatrix, w0) = 
        FeatureLearningFactorizationMachineRegressionModel.devectorize(
                modelVector.toBreeze.toDenseVector, latentDim, learnedFeatureSize)
                
    /**
     * Overloaded constructor.
     */
    def this(weights:Vector, latentDim:Int, learnedFeatureSize:Int) = 
        this(weights, HashMap[String, Any](
                FeatureLearningFactorizationMachineRegressionModel.ParamLatentDimension -> latentDim,
                FeatureLearningFactorizationMachineRegressionModel.ParamLearnFeatureSize -> learnedFeatureSize
        		))
    
    override protected def predictPoint(
            dataVector:Vector, 
            modelVector: Vector): Double = {
        
        //transform data
        val brzDataVector = dataVector.toBreeze
        val brzLearnedFeatureVector = pMatrix.t * brzDataVector //learned feature  
        
        val brzLatDataVector = vMatrix.t * brzLearnedFeatureVector
        
        //compute prediction
        (wVector dot brzLatDataVector) + (brzLatDataVector dot brzLatDataVector) + w0
    }
}

/**
 * Defines vectorization and devectorization, as well as updater and gradient functions. n
 */
object FeatureLearningFactorizationMachineRegressionModel{
    val ParamLatentDimension  = "ParamLatentDimension"
    val ParamLearnFeatureSize = "ParamLearnFeatureSize"
    
    /**
     * 
     * 
     * @param (pMatrix, wVector, vMatrix, w0) 
     */
    def devectorize(modelVect:BDV[Double], latentDim:Int, learnedFeatureSize:Int): 
    	(BDM[Double],BDV[Double], BDM[Double], Double) = {
        
        val mLen = modelVect.size // the model length
        
        val dim:Int = (modelVect.size - 1)/learnedFeatureSize - latentDim - 1
        
        val pMatrix  = modelVect(0 until dim * learnedFeatureSize).
        					toDenseMatrix.reshape(dim, learnedFeatureSize)
        val wVector  = modelVect((dim * learnedFeatureSize) until (dim * learnedFeatureSize + learnedFeatureSize))
        val vMatrix  = modelVect((dim * learnedFeatureSize + learnedFeatureSize) until (mLen - 1)).
        					toDenseMatrix.reshape(learnedFeatureSize, latentDim)
        val w0       = modelVect(mLen - 1)
        
        (pMatrix, wVector, vMatrix, w0)
    }
    
    /**
     * @param pMatrix
     * @param wVector
     * @param vMatrix
     * @param w0
     * 
     * @return a vector representation of the model (for gradient update)  
     */
    def vectorize(pMatrix:BDM[Double],wVector:BDV[Double], vMatrix:BDM[Double], w0:Double):BDV[Double] = {
        BDV.vertcat[Double](
            BDV.vertcat[Double](
            	BDV.vertcat[Double](
            		pMatrix.toDenseVector, 	wVector
            	),
            	wVector.toDenseVector
            ),
            BDV(Array[Double](w0))
        )
    }
    
    class FeatureLearningFactorizationMachineGradient(
            val latentDim:Int, val learnedFeatureSize:Int) extends Gradient{
        override def compute(
                data:Vector, 
                label:Double, 
                weights: Vector): (Vector, Double) = {
            val brzData      = data.toBreeze
    		val brzDataDense = brzData.toDenseVector 
    		val brzModelVect = weights.toBreeze.toDenseVector
            
    		val (pMatrix, wVector, vMatrix, w0) = 
    			FeatureLearningFactorizationMachineRegressionModel.devectorize(
    					brzModelVect, latentDim, learnedFeatureSize)
    		
    		//g = P' * x
    		val gVector:BDV[Double] = pMatrix.t * brzDataDense
    		//h = V' * g
    	    val hVector:BDV[Double] = vMatrix.t * gVector
    		// loss function. 
    	    val diff:Double = gVector.dot(wVector) + hVector.dot(hVector) + w0 - label   
    	    val loss = diff * diff * 0.5
    	    
    	    // compute gradient 
    	    val pMatrixGradient:BDM[Double] = brzDataDense.toDenseMatrix * 
    	    									((wVector.toDenseMatrix.t * (2.0 * diff)) + (hVector.t * (vMatrix.t * diff)))
    	    val wVectorGradient:BDV[Double] = gVector * diff
    	    val vMatrixGradient:BDM[Double] = (gVector.toDenseMatrix * (2.0 * diff)) * hVector.toDenseMatrix.t 
    	    val w0Gradient:Double           = diff
    	    val gradient: BDV[Double]       = vectorize(pMatrixGradient, wVectorGradient, vMatrixGradient, w0Gradient)
    	    
    	    //return. 
            (Vectors.fromBreeze(gradient), loss)
        }
        
        override def compute(
                data: Vector,
    			label: Double,
    			weights: Vector,
    			cumGradient: Vector): Double = {
            
            val brzData      = data.toBreeze
    		val brzDataDense = brzData.toDenseVector 
    		val brzModelVect = weights.toBreeze.toDenseVector
            
    		val (pMatrix, wVector, vMatrix, w0) = 
    			FeatureLearningFactorizationMachineRegressionModel.devectorize(
    					brzModelVect, latentDim, learnedFeatureSize)
    		
    		//g = P' * x : reduced. 
    		val gVector:BDV[Double] = pMatrix.t * brzDataDense
    		//h = V' * g : latent feature .
    	    val hVector:BDV[Double] = vMatrix.t * gVector
    		// loss function. 
    	    val diff:Double = gVector.dot(wVector) + hVector.dot(hVector) + w0 - label   
    	    val loss = diff * diff * 0.5
    	    
    	    // compute gradient 
    	    val pMatrixGradient:BDM[Double] = brzDataDense.toDenseMatrix * 
    	    									((wVector.toDenseMatrix.t * 2.0) + (hVector.t * vMatrix.t))
    	    val wVectorGradient:BDV[Double] = gVector
    	    val vMatrixGradient:BDM[Double] = gVector.toDenseMatrix * (hVector.toDenseMatrix.t * 2.0) 
    	    val w0Gradient:Double           = 1
    	    val gradient: BV[Double]       = vectorize(pMatrixGradient, wVectorGradient, vMatrixGradient, w0Gradient)
    	    
    	    //return.
    	    brzAxpy(diff, gradient, cumGradient.toBreeze)
    	    
            loss
        }
    }
       	
    
    class FeatureLearningFactorizationMachineUpdate(
                val latentDim:Int,  learnedFeatureSize:Int, 
                l21Param: Double, l1Param: Double) extends Updater{
            
            override def compute(
		      weightsOld: Vector,
		      gradient: Vector,
		      stepSize: Double,
		      iter: Int,
		      regParam: Double
		      ):(Vector, Double) = {
                
                val thisIterStepSize = stepSize / math.sqrt(iter)
                val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
                
	            //Shrinkage (except for intercept)
	            // Here we use a small trick: replacing the intercept value back instead 
	            // of segmenting.  
	            val w0Weights = brzWeights(brzWeights.size - 1) //save the intercept weight
	            brzWeights :*= (1.0 - thisIterStepSize * regParam)//shrinkage
	            brzWeights(brzWeights.size - 1) = w0Weights //recover the intercept weight
	            
	            brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
	            brzWeights(brzWeights.size - 1) = w0Weights //recover the intercept weight
	            
	            //proximal update
	            val (pMatrix, wVector, vMatrix, w0) = 
    				FeatureLearningFactorizationMachineRegressionModel.devectorize(
    					brzWeights.toDenseVector, latentDim, learnedFeatureSize)
	            
    			//projected gradient 
                //val pMatrixProx = ProxFunctions.proximalL21(pMatrix, l21Param/thisIterStepSize)
                val pMatrixProx = ProxFunctions.proximalL21L1(pMatrix, l1Param/thisIterStepSize, l21Param/thisIterStepSize)
                val brzWeightsProx = 
                    FeatureLearningFactorizationMachineRegressionModel.vectorize(pMatrixProx, wVector, vMatrix, w0)
                
                //F-norm without w0 
                //NOTE: scala breeze 0.7 does not support brzNorm on slice 
                val norm = brzWeightsProx(0 to brzWeights.size - 2).norm(2.0)
                
                val regVal = 0.5 * regParam * norm * norm 
                			+ ProxFunctions.funcValL21L1(pMatrixProx, l1Param, l21Param)
                
                (Vectors.fromBreeze(brzWeightsProx), regVal)
            }
        }
	    
    
}

/**
 * Algorithms for training Factorization Machine with L2 regularization using spark SGD.
 */
class FactorizationMachineRegressionFeatureLearningWithSGD private (
        private var latentDim: Int,
        private var l21RegParam: Double,
        private var l1RegParam: Double,
        private var learnedFeatureSize: Int,
		private var stepSize: Double,
		private var numIterations: Int,
		private var regParam: Double,
		private var miniBatchFraction: Double)
  extends CustomizedAlgorithm[FeatureLearningFactorizationMachineRegressionModel] with Serializable {
	
    private val gradient:Gradient = 
        new FeatureLearningFactorizationMachineRegressionModel.
        	FeatureLearningFactorizationMachineGradient(latentDim:Int, learnedFeatureSize)
    private val updater:Updater  = 
        new FeatureLearningFactorizationMachineRegressionModel.
        	FeatureLearningFactorizationMachineUpdate(latentDim,  learnedFeatureSize, l21RegParam, l1RegParam)
    
    override val optimizer = new GradientDescent(gradient, updater)
        .setStepSize(stepSize)
        .setNumIterations(numIterations)
        .setRegParam(regParam)
        .setMiniBatchFraction(miniBatchFraction)
    
    /**
     * Construct a RidgeRegression object with default parameters: {stepSize: 1.0, numIterations: 100,
     * regParam: 1.0, miniBatchFraction: 1.0}.
     */
    def this(latentDim: Int) = this(latentDim, 0.01, 0.01,
            					latentDim * 2, 0.0001, 100, 1.0, 1.0)    
    
	override protected def createModel(weights: Vector, param:HashMap[String, Any] ) = {
		new FeatureLearningFactorizationMachineRegressionModel(weights, param)
	}
}

/**
 * Top-level methods for calling FactorizationMachineRegression 
 */
object FactorizationMachineRegressionFeatureLearningWithSGD{
    def train(
            input: RDD[LabeledPoint],
            latentDim: Int,
            l21RegParam: Double,
            l1RegParam: Double,
            learnedFeatureSize: Int,
            numIterations: Int,
            stepSize: Double,
            regParam: Double,
            miniBatchFraction: Double,
            initialWeights: Vector): FeatureLearningFactorizationMachineRegressionModel = {
        
        new FactorizationMachineRegressionFeatureLearningWithSGD(
                latentDim, l21RegParam, l1RegParam, learnedFeatureSize, stepSize, numIterations, regParam, miniBatchFraction).run(
                input, initialWeights, HashMap[String, Any](
                FactorizationMachineRegressionModel.ParamLatentDimension -> latentDim))
    }
    
    def train(
            input: RDD[LabeledPoint],
            latentDim: Int,
            l21RegParam: Double,
            l1RegParam: Double,
            learnedFeatureSize: Int,
            numIterations: Int,
            stepSize: Double,
            regParam: Double,
            miniBatchFraction: Double): FeatureLearningFactorizationMachineRegressionModel = {
        
        val featureDim:Int = input.first.features.size
        val initialWeights: Vector = FactorizationMachineRegressionModel.initUnitModel(featureDim, latentDim)
        
        FactorizationMachineRegressionFeatureLearningWithSGD.train(
                input, latentDim, l21RegParam, l1RegParam, learnedFeatureSize, numIterations, stepSize, regParam, miniBatchFraction, initialWeights)
        
    }
    
}