package org.apache.spark.mllib.optimization

import scala.collection.immutable.HashMap

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.RegressionModel
import org.apache.spark.mllib.regression.LabeledPoint

import breeze.linalg.{norm => brzNorm, DenseMatrix => BDM, axpy => brzAxpy, Vector=> BV, DenseVector=> BDV}

/**
 * The factorization machine regression model. This class defines the 
 * data structure used to prediction. The computation components are 
 * defined in the companion object.  
 * 
 * The factorization machine explicitly models feature interactions 
 * using a bilinear model in addition to the linear model. This is 
 * equivalent to a quadratic term with a low rank interaction matrix. 
 * 
 * y = x' * w + x' * R * x + w0
 * 
 * where R = V * V' is the low rank feature interaction matrix. For 
 * more details see the paper. 
 * [[http://www.ismll.uni-hildesheim.de/pub/pdfs/Rendle2010FM.pdf]]
 * 
 * @param modelVector a vector represent the entire model 
 * @param param the parameter hashmap with a `ParamLatentDimension` key, 
 * 				corresponding to an integer representing the latent 
 *     			dimensionality of the interaction matrix.   
 */
class FactorizationMachineRegressionModel (
        override val modelVector: Vector,
        override val param:HashMap[String, Any]) 
        extends CustomizedModel(modelVector, param)
		with RegressionModel with Serializable{
    
    //extract parameters
    val latentDim:Int = param(FactorizationMachineRegressionModel.ParamLatentDimension).asInstanceOf[Int]
    //devectorized model 
    val (wVector:BDV[Double], vMatrix:BDM[Double], w0:Double) = 
        FactorizationMachineRegressionModel.devectorize(modelVector.toBreeze.toDenseVector, this.latentDim)
    
    /**
     * Overloaded constructor. 
     */
    def this(weights:Vector, latentDim:Int) = 
        this(weights, HashMap[String, Any](
                FactorizationMachineRegressionModel.ParamLatentDimension -> latentDim))
    
    override protected def predictPoint(
            dataVector:Vector, 
            modelVector: Vector): Double = {
        
        // transform data
        val brzDataVector = dataVector.toBreeze
        val brzLatDataVector = vMatrix.t * brzDataVector
        // compute prediction
        (wVector dot brzDataVector) + (brzLatDataVector dot brzLatDataVector) + w0 
    }
    
}

/**
 * Defines vectorization and devectorization, as well as updater and gradient functions. 
 */
object FactorizationMachineRegressionModel {
    val ParamLatentDimension = "ParamLatentDimension" 
    
    /**
     * Devectorize an integrated model vector into a vector and a matrix 
     * 
     * @param modelVect the model vector
     * @param dim the dimensionality of feature
     * 
     * @return (weight, vMatrix, w0)
     */
    def devectorize(modelVect:BDV[Double], latentDim:Int): (BDV[Double], BDM[Double], Double) = {
        require((modelVect.size - 1) % (latentDim + 1) == 0)
        
        //The latent dimension
        
        val dim:Int = (modelVect.size - 1)/(latentDim + 1) 
        
        val wVector = modelVect(0 until dim) 
        val vMatrix = modelVect(dim until modelVect.size - 1).toDenseMatrix.reshape(dim, latentDim)
        val w0      = modelVect(modelVect.size-1)
        (wVector, vMatrix, w0)
    }
    
    /**
     * Vectorize a vector and a matrix into an integrated model vector
     * 
     * [wVector, vMatrix.toDenseVector, w0]'
     * 
     * @param wVector the linear model
     * @param vMatrix the low rank component of interaction 
     * @param w0 the intercept
     * 
     * @return a vector representation of the model 
     */
    def vectorize(wVector:BDV[Double], vMatrix:BDM[Double], w0:Double): BDV[Double] = {
        BDV.vertcat[Double](
                	BDV.vertcat[Double](wVector.toDenseVector, vMatrix.toDenseVector),
                	BDV(Array[Double](w0))
                )
    }
    
    /**
     * Defines gradient computation
     * 
     * @param latentDim the latent dimension of the rank of interaction matrix
     */
    class FactorizationMachineGradient (val latentDim:Int) extends Gradient{
    	override def compute(data: Vector, label: Double, weights: Vector): (Vector, Double) = {
    		val brzData      = data.toBreeze
    		val brzDataDense = brzData.toDenseVector 
    		val brzModelVect = weights.toBreeze.toDenseVector
    		val (wVector:BDV[Double], vMatrix:BDM[Double], w0:Double) = devectorize(brzModelVect, latentDim)
    	    
    		val zVector:BDV[Double] = vMatrix.t * brzData
    		val diff:Double = wVector.dot(brzData) + zVector.dot(zVector) + w0 - label
    		
    		//gradient
    		val wVectorGradient:BDV[Double] = brzDataDense * diff
    		val vMatrixGradient:BDM[Double] = brzDataDense.toDenseMatrix.t * (zVector.toDenseMatrix * (diff * 2.0))
    		val w0Gradient:Double           = diff
    		val gradient:BDV[Double]        = vectorize(wVectorGradient, vMatrixGradient, w0Gradient)
    		//loss
    		val loss = diff * diff * 0.5
    		
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
    		val (wVector:BDV[Double], vMatrix:BDM[Double], w0:Double) = devectorize(brzModelVect, latentDim)
    	    
    		val zVector:BDV[Double] = vMatrix.t * brzData
    		val diff:Double = wVector.dot(brzData) + zVector.dot(zVector) + w0 - label
    		
    		//gradient
    		//here we don't multiply diff until axpy. 
    		val wVectorGradient:BDV[Double] = brzDataDense
    		val vMatrixGradient:BDM[Double] = brzDataDense.toDenseMatrix.t * (zVector.toDenseMatrix * 2.0)
    		val w0Gradient:Double           = 1
    		val gradient:BV[Double]         = vectorize(wVectorGradient, vMatrixGradient, w0Gradient)
    		
    		brzAxpy(diff, gradient, cumGradient.toBreeze)
    		//loss
    		diff * diff * 0.5
    	}
    }
    
    /**
     * Defines L1 projected gradient update rule 
     * 
     * @param latentDim the latent dimension of the rank of interaction matrix
     */
    class FactorizationMachineL1Update (val latentDim:Int) extends Updater{
        override def compute(
	      weightsOld: Vector,
	      gradient: Vector,
	      stepSize: Double,
	      iter: Int,
	      regParam: Double):(Vector, Double) = {
            throw new NotImplementedError()
        }
    }
    
    /**
     * Defines L2 gradient update rule. This rule does not penalize the intercept. 
     * 
     * @param latentDim the latent dimension of the rank of interaction matrix
     */
    class FactorizationMachineL2Update (val latentDim:Int) extends Updater{
        override def compute(
	      weightsOld: Vector,
	      gradient: Vector,
	      stepSize: Double,
	      iter: Int,
	      regParam: Double):(Vector, Double) = {
            val thisIterStepSize = stepSize / math.sqrt(iter)
            val brzWeights: BV[Double] = weightsOld.toBreeze.toDenseVector
            
            
            //gradient update 
            // add up both updates from the gradient of the loss (= step) as well as
		    // the gradient of the regularizer (= regParam * weightsOld)
		    // w' = w - thisIterStepSize * (gradient + regParam * w)
		    //    = (1 - thisIterStepSize * regParam) * w - thisIterStepSize * gradient
            
            //Shrinkage (except for intercept)
            // Here we use a small trick: replacing the intercept value back instead 
            // of segmenting.  
            val w0Weights = brzWeights(brzWeights.size - 1) //save the intercept weight
            brzWeights :*= (1.0 - thisIterStepSize * regParam)//shrinkage
            brzWeights(brzWeights.size - 1) = w0Weights //recover the intercept weight
            
            brzAxpy(-thisIterStepSize, gradient.toBreeze, brzWeights)
    		val norm = brzNorm(brzWeights(0 to brzWeights.size - 2), 2.0) //norm without w0

    		(Vectors.fromBreeze(brzWeights), 0.5 * regParam * norm * norm)
        }
    }
}

/**
 * Algorithms for training Factorization Machine with L2 regularization using spark SGD.
 */
class FactorizationMachineRegressionL2WithSGD private (
        private var latentDim: Int,
		private var stepSize: Double,
		private var numIterations: Int,
		private var regParam: Double,
		private var miniBatchFraction: Double)
  extends CustomizedAlgorithm[FactorizationMachineRegressionModel] with Serializable {
	
    private val gradient:Gradient = 
        new FactorizationMachineRegressionModel.FactorizationMachineGradient(latentDim)
    private val updater:Updater  = 
        new FactorizationMachineRegressionModel.FactorizationMachineL2Update(latentDim)
    
    override val optimizer = new GradientDescent(gradient, updater)
        .setStepSize(stepSize)
        .setNumIterations(numIterations)
        .setRegParam(regParam)
        .setMiniBatchFraction(miniBatchFraction)
    
    /**
     * Construct a RidgeRegression object with default parameters: {stepSize: 1.0, numIterations: 100,
     * regParam: 1.0, miniBatchFraction: 1.0}.
     */
    def this(latentDim: Int) = this(latentDim, 0.0001, 100, 1.0, 1.0)    
    
	override protected def createModel(weights: Vector, param:HashMap[String, Any] ) = {
		new FactorizationMachineRegressionModel(weights, param)
	}
}

/**
 * Top-level methods for calling FactorizationMachineRegression 
 */
object FactorizationMachineRegressionL2WithSGD{
    def train(
            input: RDD[LabeledPoint],
            latentDim: Int,
            numIterations: Int,
            stepSize: Double,
            regParam: Double,
            miniBatchFraction: Double,
            initialWeights: Vector) = {
        new FactorizationMachineRegressionL2WithSGD(latentDim, stepSize, numIterations, regParam, miniBatchFraction).run(
                input, initialWeights, HashMap[String, Any](
                FactorizationMachineRegressionModel.ParamLatentDimension -> latentDim))
    }
    
}
