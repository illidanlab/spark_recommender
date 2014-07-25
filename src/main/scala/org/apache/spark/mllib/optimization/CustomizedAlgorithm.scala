package org.apache.spark.mllib.optimization

import scala.collection.immutable.HashMap

import org.apache.spark.{Logging, SparkException}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.optimization._
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}

/**
 * A general model, where the variables (weights) may have some structures 
 * defined by given parameters. 
 * 
 * Since the optimization algorithms take vector input, the model parameters 
 * should be able to vectorized and devectorized. To create customized models,
 * the key is to define vectorization and devectorization, and then define function
 * value and gradient computations associated with the model. 
 * 
 * This data structure is created from the output of optimization algorithm
 * using a vectorized model, and all information necessary to devectorize the 
 * model. 
 * 
 * @param modelVector the vectorized model
 * @param param the parameters related to devectorization. 
 */
@DeveloperApi
abstract class CustomizedModel(
        val modelVector:Vector, 
        val param:HashMap[String, Any])
	extends Serializable{
    
    /**
     * Predict the result given a data point and the weights learned.
     *
     * @param dataMatrix Row vector containing the features for this data point
     * @param weightMatrix Column vector containing the weights of the model
     */
    protected def predictPoint(dataMatrix: Vector, weightVector: Vector): Double

    /**
     * Predict values for the given data set using the model trained.
     *
     * @param testData RDD representing data points to be predicted
     * @return RDD[Double] where each entry contains the corresponding prediction
     */
    def predict(testData: RDD[Vector]): RDD[Double] = {
        // A small optimization to avoid serializing the entire model. Only the weightsMatrix
        // and intercept is needed.
        val localWeights = modelVector

        testData.map(v => predictPoint(v, localWeights))
    }

    /**
     * Predict values for a single data point using the model trained.
     *
     * @param testData array representing a single data point
     * @return Double prediction from the trained model
     */
    def predict(testData: Vector): Double = {
        predictPoint(testData, modelVector)
    }
    
}

/**
 * A customized algorithm, which includes an optimization algorithm, computes 
 * models and returns in the form of [[org.apache.spark.mllib.optimization.CustomizedModel]]
 * 
 * The algorithm implementations may have multiple customized run functions
 * taking different parameters.  
 */
@DeveloperApi
abstract class CustomizedAlgorithm [M <: CustomizedModel]
	extends Logging with Serializable{
	
    protected val validators: Seq[RDD[LabeledPoint] => Boolean] = List()

    /** The optimizer to solve the problem. */
    def optimizer: Optimizer

    protected var validateData: Boolean = true

    /**
     * Create a model given the model weights 
     */
    protected def createModel(weights: Vector, param:HashMap[String, Any]): M

    /**
     * Set if the algorithm should validate data before training. Default true.
     */
    def setValidateData(validateData: Boolean): this.type = {
        this.validateData = validateData
        this
    }

    /**
     * Run the algorithm with the configured parameters on an input RDD
     * of LabeledPoint entries starting from the initial weights provided.
     * 
     * @param input the data sample
     * @param initialWeights the starting point of the optimization. 
     * @param param structure information of the model for devectorization.
     */
    def run(input: RDD[LabeledPoint], initialWeights: Vector, param:HashMap[String, Any]): M = {

        // Check the data properties before running the optimizer
        if (validateData && !validators.forall(func => func(input))) {
            throw new SparkException("Input validation failed.")
        }

        val data = input.map(labeledPoint => (labeledPoint.label, labeledPoint.features))

        val weights = optimizer.optimize(data, initialWeights)

        createModel(weights, param)
  	}
    
}