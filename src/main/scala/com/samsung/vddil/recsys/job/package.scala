package com.samsung.vddil.recsys

/**
 * Defines a list of recommendation jobs in the pipeline
 * 
 * ===Overview===
 * The main trait is given by [[com.samsung.vddil.recsys.job.Job]]. For each job running 
 * in the recommendation pipeline, there is a corresponding job class extending [[com.samsung.vddil.recsys.job.Job]].
 * Each job definition should associate an implementation of [[com.samsung.vddil.recsys.job.JobStatus]],
 * which stores completed stages of the job as well as references to the resources (data, features, models)
 * produced in the job. 
 * 
 * A toy implementation is given by [[com.samsung.vddil.recsys.job.HelloWorldJob]], while the 
 * learning to rank recommendation is defined in [[com.samsung.vddil.recsys.job.RecJob]]. It is 
 * easy to define other jobs. For example, traditional factorization-based recommendation. 
 * 
 */
import java.util.Date
import java.text.SimpleDateFormat
import java.util.GregorianCalendar
import java.util.Calendar
import com.samsung.vddil.recsys.model.SerializableModel
import scala.xml.Node
import com.samsung.vddil.recsys.evaluation.RecJobMetric
import com.samsung.vddil.recsys.utils.Logger
import scala.collection.mutable.HashMap
import com.samsung.vddil.recsys.evaluation.RecJobMetricMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricRMSE
import com.samsung.vddil.recsys.evaluation.RecJobMetricHR
import com.samsung.vddil.recsys.evaluation.RecJobMetricColdRecall
package object job{
    
    /**
     * Expand a date string into a list of strings.
     * 
     * e.g., '20140312' returns ['20140312']
     *       '20140320-20140322' returns ['20140320', '20140321', '20140322'] 
     * 
     * @param dateStr the string to be expanded
     * @param the formatter of the date string. Default : yyyyMMdd
     * @param dateRangeSplit, the delimiter used to split start date and end date. Default: '-'
     * 
     */
    def expandDateList(
            dateStr:String, 
            dateParser: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd"), 
            dateRangeSplit:Char = '-'):Seq[String] = {
        var result = Array[String]()
        
        val dateSplits = dateStr.split(dateRangeSplit)
        dateSplits.size match {
            case 1 => result = result :+ dateSplits(0) 
            case 2 => //multiple presents  
                val date1 = dateParser.parse(dateSplits(0))
                val date2 = dateParser.parse(dateSplits(1))
                if (date2.after(date1)){
                    result = result ++ dateInterval(date1, date2).map(dateParser.format(_))
                }else{
                    result = result ++ dateInterval(date2, date1).map(dateParser.format(_))
                }
            case _ => //incorrect presentation.
                //Logger.warn("Incorrect date string found: "+ dateStr)
        }
        
        result
    }
    
    /**
     * Compute the interval of two dates. 
     * 
     * @param initDate the starting date (inclusive)
     * @param finalDate the end date (inclusive if includeFinalDate is true)
     * @param includeFinalDate if the final date will be included. 
     */
    def dateInterval(initDate:Date , finalDate:Date, includeFinalDate:Boolean = true):Array[Date] = {
	    var dates = Array[Date]();
	    
	    val calendar: Calendar = new GregorianCalendar();
	    calendar.setTime(initDate);
	
	    while (calendar.getTime().before(finalDate))
	    {
	        val resultado:Date = calendar.getTime();
	        dates = dates :+ resultado
	        calendar.add(Calendar.DATE, 1);
	    }
	    
	    if(includeFinalDate)
	    	dates :+ finalDate
	    else
	    	dates
	}
    
    /**
     * Formats the summary file and output using an existing BufferedWriter. 
     */
    def outputSummaryFile(job:RecJob, writer: java.io.BufferedWriter){
        
        var headnum_lv1 = 0
        var headnum_lv2 = 0
        
        def writeline(str:String) = {
        		writer.write(str); writer.newLine() }
        def writehead(str:String, level:Int){
            if(level == 1){
                headnum_lv1 += 1
                headnum_lv2 = 0
                writer.write(headnum_lv1 + ". ");
            	writer.write(str); writer.newLine()
            	writer.write("======="); writer.newLine()
            	writer.newLine()
            }else if(level == 2){
                headnum_lv2 += 1
                writer.write(headnum_lv1 + "." + headnum_lv2 + ". ")
            	writer.write(str); writer.newLine()
            	writer.write("-------"); writer.newLine()
            }else if(level ==3){
                writer.write("###" + str); writer.newLine()
            }
        }
        
        // start writing files
        writeline("===RecJob Summary START===")
        writer.newLine()
        
        writehead("Job Properties", 1)
        
        writeline("Job name:"        + job.jobName)
        writeline("Job description:" + job.jobDesc)
        writeline("Train Dates: " + job.trainDates.mkString("[", ",", "]"))
        //writeline("  User number: " + this.jobStatus.users.size)
        //writeline("  Item number: " + this.jobStatus.items.size)
        writeline("Test Dates: "  + job.testDates.mkString("[", ",", "]"))
        writer.newLine()
        
        /// training watchtime data 
        writehead("Combined Datasets", 1)
        
        writehead("Training watchtime data", 2)
        if (job.jobStatus.resourceLocation_CombinedData_train.isDefined){
            val trainCombData = job.jobStatus.resourceLocation_CombinedData_train.get
            writeline(" Data Identity: " + trainCombData.resourceStr)
            writeline(" Data File:     " + trainCombData.resourceLoc)
            writeline(" Data Dates:    " + trainCombData.dates.mkString("[",", ","]"))
            writeline("   User Number: " + trainCombData.userNum)
            writeline("   Item Number: " + trainCombData.itemNum)
        }
        writer.newLine()
        
        /// features
        writehead("Features", 1)
        
        writehead("User Features", 2)
        for ((featureId, feature) <- job.jobStatus.resourceLocation_UserFeature){
            writeline("  Feature Identity:   " + feature.resourceStr + " (OID:" + System.identityHashCode(feature) + ")")
            writeline("  Feature Parameters: " + feature.featureParams.toString)
            writeline("  Feature File:       " + feature.featureFileName)
            writeline("     Feature Size:  " + feature.featureSize)
            feature.featurePostProcessor.foreach{processor =>
                writeline("     " + processor.toString)
            }
            writer.newLine()
        }
        writer.newLine()
        writehead("Item Features", 2)
        for ((featureId, feature) <- job.jobStatus.resourceLocation_ItemFeature){
            writeline("  Feature Identity:   " + feature.resourceStr + " (OID:" + System.identityHashCode(feature) + ")")
            writeline("  Feature Parameters: " + feature.featureParams.toString)
            writeline("  Feature File:       " + feature.featureFileName)
            writeline("     Feature Size:  " + feature.featureSize)
            feature.featurePostProcessor.foreach{processor =>
                writeline("     " + processor.toString)
            }
            writer.newLine()
        }
        writer.newLine()
        
        /// assembled data
        writehead("Assembled Continuous Datasets", 1)
        for((adataId, data) <- job.jobStatus.resourceLocation_AggregateData_Continuous){
            writeline("  Data Identity:      " + data.resourceStr)
            writeline("  Data File:          " + data.resourceLoc)
            writeline("  Data Size:          " + data.size)
            writeline("  Data Dimension:     " + data.dimension)
            writeline("  User Features:")
            for (feature <- data.userFeatureOrder){
                writeline("     Feature Name:  " + feature.featureIden  + " (OID:" + System.identityHashCode(feature) + ")")
                writeline("     Feature Iden:  " + feature.resourceStr)
                writeline("     Feature Size:  " + feature.featureSize)
                writeline("     Feature Param: " + feature.featureParams.toString)
                feature.featurePostProcessor.foreach{processor =>
	                writeline("         " + processor.toString)
	            }
            }
            writeline("  Item Features:")
            for (feature <- data.itemFeatureOrder){
                writeline("     Feature Name: " + feature.featureIden  + " (OID:" + System.identityHashCode(feature) + ")")
                writeline("     Feature Iden: " + feature.resourceStr)
                writeline("     Feature Size: " + feature.featureSize)
                writeline("     Feature Param: " + feature.featureParams.toString)
                feature.featurePostProcessor.foreach{processor =>
	                writeline("         " + processor.toString)
	            }
            }
            writer.newLine()
        }
        
        /// models 
        writehead("Models", 1)
        
        writehead("Regression Models", 2)
        for ((modelId, model) <- job.jobStatus.resourceLocation_RegressModel){
            writeline("  Model Name:     " + model.modelName)
            writeline("  Model Dim:      " + model.modelDimension)
            writeline("  Model Identity: " + modelId)
            writeline("  Model Param:    " + model.modelParams.toString)
            writeline("  Model DataRI:   " + model.learnDataResourceStr)
            if (model.isInstanceOf[SerializableModel[_]])
            	writeline("  Model Files:    " + model.asInstanceOf[SerializableModel[_]].modelFileName )
            writer.newLine()
        }
        writer.newLine()
        writehead("Classification Models", 2)
        for ((modelId, model) <- job.jobStatus.resourceLocation_ClassifyModel){
            writeline("  Model Name:     " + model.modelName)
            writeline("  Model Dim:      " + model.modelDimension)
            writeline("  Model Identity: " + modelId)
            writeline("  Model Param:    " + model.modelParams.toString)
            writeline("  Model DataRI:   " + model.learnDataResourceStr)
            if (model.isInstanceOf[SerializableModel[_]])
            	writeline("  Model Files:    " + model.asInstanceOf[SerializableModel[_]].modelFileName )
            writer.newLine()
        }
        writer.newLine()
        
        /// tests
        writehead("Tests", 1)
        
        for ((model, testList) <- job.jobStatus.completedTests){
            writehead("Model: " + model.resourceStr, 2)
            
            writeline("  Model Name:     " + model.modelName)
            writeline("  Model Param:    " + model.modelParams.toString)
            writeline("  Model Test List: ")
            
            for ((testUnit, results) <- testList){
                writeline("    Test Unit Class:      " + testUnit.getClass().getName())
                writeline("    Test Unit Identity:   " + testUnit.resourceIdentity)
                writeline("    Test Unit Parameters: " + testUnit.testParams.toString)
                
                writeline("    Test Metric List: ")
                for((metric, metricResult )<- results){
		            writer.write("      Metric Resource Identity: " + metric.resourceIdentity); writer.newLine()
		            writer.write("      Metric Parameters:        " + metric.metricParams.toString); writer.newLine()
		            for((resultStr, resultVal) <- metricResult) {
		                writer.write("        [" + resultStr + "] " + resultVal.formatted("%.4g")); writer.newLine()
		            }
		        }
            }
            
            writer.newLine()
        }
        writer.newLine()
        
        writeline("===RecJob Summary END===")

    }    
 
    
    /**
     * Formats the summary file and output using an existing BufferedWriter. 
     */
    def outputSummaryFile(job:RecMatrixFactJob, writer: java.io.BufferedWriter){
        
        var headnum_lv1 = 0
        var headnum_lv2 = 0
        
        def writeline(str:String) = {
        		writer.write(str); writer.newLine() }
        def writehead(str:String, level:Int){
            if(level == 1){
                headnum_lv1 += 1
                headnum_lv2 = 0
                writer.write(headnum_lv1 + ". ");
            	writer.write(str); writer.newLine()
            	writer.write("======="); writer.newLine()
            	writer.newLine()
            }else if(level == 2){
                headnum_lv2 += 1
                writer.write(headnum_lv1 + "." + headnum_lv2 + ". ")
            	writer.write(str); writer.newLine()
            	writer.write("-------"); writer.newLine()
            }else if(level ==3){
                writer.write("###" + str); writer.newLine()
            }
        }
        
        // start writing files
        writeline("===RecJob Summary START===")
        writer.newLine()
        
        writehead("Job Properties", 1)
        
        writeline("Job name:"        + job.jobName)
        writeline("Job description:" + job.jobDesc)
        writeline("Train Dates: " + job.trainDates.mkString("[", ",", "]"))
        //writeline("  User number: " + this.jobStatus.users.size)
        //writeline("  Item number: " + this.jobStatus.items.size)
        writeline("Test Dates: "  + job.testDates.mkString("[", ",", "]"))
        writer.newLine()
        
        /// training watchtime data 
        writehead("Combined Datasets", 1)
        
        writehead("Training watchtime data", 2)
        if (job.jobStatus.resourceLocation_CombinedData_train.isDefined){
            val trainCombData = job.jobStatus.resourceLocation_CombinedData_train.get
            writeline(" Data Identity: " + trainCombData.resourceStr)
            writeline(" Data File:     " + trainCombData.resourceLoc)
            writeline(" Data Dates:    " + trainCombData.dates.mkString("[",", ","]"))
            writeline("   User Number: " + trainCombData.userNum)
            writeline("   Item Number: " + trainCombData.itemNum)
        }
        writer.newLine()
        
        writehead("Testing watchtime data (only used in MSE/RMSE)", 2)
        if (job.jobStatus.resourceLocation_CombinedData_test.isDefined){
            val testCombData = job.jobStatus.resourceLocation_CombinedData_test.get
            writeline(" Data Identity: " + testCombData.resourceStr)
            writeline(" Data File:     " + testCombData.resourceLoc)
            writeline(" Data Dates:    " + testCombData.dates.mkString("[",", ","]"))
            writeline("   User Number: " + testCombData.userNum)
            writeline("   Item Number: " + testCombData.itemNum)
        }
        writer.newLine()
        
        /// features
        writehead("Features", 1)
        
//        writehead("User Features", 2)
//        for ((featureId, feature) <- job.jobStatus.resourceLocation_UserFeature){
//            writeline("  Feature Identity:   " + feature.resourceStr + " (OID:" + System.identityHashCode(feature) + ")")
//            writeline("  Feature Parameters: " + feature.featureParams.toString)
//            writeline("  Feature File:       " + feature.featureFileName)
//            writeline("     Feature Size:  " + feature.featureSize)
//            feature.featurePostProcessor.foreach{processor =>
//                writeline("     " + processor.toString)
//            }
//            writer.newLine()
//        }
//        writer.newLine()
//        writehead("Item Features", 2)
//        for ((featureId, feature) <- job.jobStatus.resourceLocation_ItemFeature){
//            writeline("  Feature Identity:   " + feature.resourceStr + " (OID:" + System.identityHashCode(feature) + ")")
//            writeline("  Feature Parameters: " + feature.featureParams.toString)
//            writeline("  Feature File:       " + feature.featureFileName)
//            writeline("     Feature Size:  " + feature.featureSize)
//            feature.featurePostProcessor.foreach{processor =>
//                writeline("     " + processor.toString)
//            }
//            writer.newLine()
//        }
//        writer.newLine()
        
        
        /// models 
        writehead("Models", 1)
        
        writehead("Regression Models", 2)
        for ((modelId, model) <- job.jobStatus.resourceLocation_models){
            writeline("  Model Name:     " + model.modelName)
            //writeline("  Model Dim:      " + model.modelDimension)
            writeline("  Model Identity: " + modelId)
            writeline("  Model Param:    " + model.modelParams.toString)
            //writeline("  Model DataRI:   " + model.learnDataResourceStr)
            if (model.isInstanceOf[SerializableModel[_]])
            	writeline("  Model Files:    " + model.asInstanceOf[SerializableModel[_]].modelFileName )
            writer.newLine()
        }
        writer.newLine()
        
        /// tests
        writehead("Tests", 1)
        
        for ((model, testList) <- job.jobStatus.completedTests){
            writehead("Model: " + model.resourceStr, 2)
            
            writeline("  Model Name:     " + model.modelName)
            writeline("  Model Param:    " + model.modelParams.toString)
            writeline("  Model Test List: ")
            
            for ((testUnit, results) <- testList){
                writeline("    Test Unit Class:      " + testUnit.getClass().getName())
                writeline("    Test Unit Identity:   " + testUnit.resourceIdentity)
                writeline("    Test Unit Parameters: " + testUnit.testParams.toString)
                
                writeline("    Test Metric List: ")
                for((metric, metricResult )<- results){
		            writer.write("      Metric Resource Identity: " + metric.resourceIdentity); writer.newLine()
		            writer.write("      Metric Parameters:        " + metric.metricParams.toString); writer.newLine()
		            for((resultStr, resultVal) <- metricResult) {
		                writer.write("        [" + resultStr + "] " + resultVal.formatted("%.4g")); writer.newLine()
		            }
		        }
            }
            
            writer.newLine()
        }
        writer.newLine()
        
        writeline("===RecJob Summary END===")

    }        

}