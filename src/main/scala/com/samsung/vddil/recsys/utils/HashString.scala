package com.samsung.vddil.recsys.utils

import java.security.MessageDigest

/**
 * Provides a hasing from string using specified hashing algorithm. 
 * 
 * @author jiayu.zhou
 */
object HashString {
  
	val HashType_MD5    = "MD5"
	val HashType_SHA1   = "SHA-1"
	val HashType_SHA256 = "SHA-256"
	
	/** 
	 * Hashing a plain string using desired hashing  
	 * 
	 * hashContent: the original String
	 * hashType: hashing algorithm. 
	 *           Options: HashString.HashType_MD5
	 *           	      HashString.HashType_SHA1
	 *                    HashString.HashType_SHA256
	 * 
	 * Example:
	 * 		hashContent: "123456"
	 *   	hashType:    HashType_MD5
	 * 		Result:      "e10adc3949ba59abbe56e057f20f883e"
	 */
	def generateHash(hashContent:String, hashType:String = HashType_MD5):String = {
		
	    var md:MessageDigest  = MessageDigest.getInstance(hashType)
        md.update(hashContent.getBytes())
        var byteData:Array[Byte] = md.digest()
  
        //use StringBuffer to concatenate the bytes. 
        
        var hexString:StringBuffer = new StringBuffer()
	    
	    var i = 0
	    for (i <- 0 to byteData.length - 1){
	     
	    	var hex:String = Integer.toHexString(0xff & byteData(i))
	    	
	        if(hex.length()==1) hexString.append('0')
	        
	        hexString.append(hex)
	    }
	    
	    hexString.toString()
	}
}