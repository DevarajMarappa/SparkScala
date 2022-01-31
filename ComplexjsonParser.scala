package jsonParse

import org.apache.log4j._

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf,SparkContext}


object ComplexjsonParser {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
      val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkChargePoints")
    .getOrCreate()
/*    
    val conf = new SparkConf().setMaster("local").setAppName("SparkChargePoints")
      val sc = SparkContext.getOrCreate(conf)
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)*/
    
    def main(args:Array[String]){  
      println("started")
      val input = "C://Users//devaraj_gowda//Downloads/Softwares/software/Workspace/ScalaSQLServer/src/jsonParse/complexJson.json"
        //"C://Users//devaraj_gowda//Downloads//csv_files//input//electric-chargepoints-2017.csv"
      
      //C:\Users\devaraj_gowda\Downloads\Softwares\software\Workspace\
      val df = spark.read.option("multiline", "true").json(input)
      println(df.show())
      println(df.printSchema())
      
    //available columns
   //   val columns:Array[String]=df.columns;
      val columns  = df.columns      
      for(m1 <- columns)
      {
         println(m1)
      }
     
      //list for the final column names
//       var col_names: ListBuffer[String] = ListBuffer()
      import scala.collection.mutable.ListBuffer
      var col_names = new ListBuffer[String]()
      
      //checking data type of each column and adding to the list of final column name
      for(col<-columns)
      {
        // if the col is struct type ..get all fields
      import org.apache.spark.sql.types.StructType
      if(df.schema(col).dataType.isInstanceOf[StructType])
      { 
        for(field1<-df.schema(col).dataType.asInstanceOf[StructType].fields) 
          {     // using . to form the column name
                col_names += col+"."+field1.name
          }
      }
      else
      // not a struct col so same col name can be used
      col_names += col      
      }
      
     //Replace . with other symbol
      val colList=col_names.map(name=>col(name).as(name.replace('.','_')))
      
      val outputDF=df.select(colList:_*)
      outputDF.printSchema()
      outputDF.show      
      
      
/*      
     val localpath="C://APIAuditLog_CCE_archive.json"
    val logslocakDF = spark.read.option("multiline", "true").json(localpath)
    println(logslocakDF.show)
    println("Total logs count : "+logslocakDF.count())
     println(logslocakDF.printSchema())*/
    
    }
}