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
}