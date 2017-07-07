package com.lanny.analysistest.util

import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.fs.Path

class RegexExcludePathFilter(val regex:String) extends  PathFilter {  
    

  def accept(path:Path):Boolean={  
    !path.toString().matches(regex)  
  }  
}