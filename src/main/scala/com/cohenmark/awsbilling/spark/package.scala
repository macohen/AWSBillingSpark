/*
Copyright 2014 Mark Cohen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.cohenmark.awsbilling
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SchemaRDD, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{IntegerType, StringType, StructField, StructType}

/**
 * Created by cohenma on 11/30/14.
 */
object spark {
  def main(args: Array[String]) {

    val conf:SparkConf = new SparkConf()
      //.setMaster("spark://NY-COHEN-LI.local:7077")

      .setMaster("local[2]")
      .setAppName("Summarize AWS Billing")
      .setSparkHome("/Users/cohenma/Downloads/spark-1.1.0")
      .setJars(Seq("target/scala-2.10/awsbillingspark_2.10-1.0.jar"))

    val sparkContext:SparkContext = new SparkContext(conf)

    val sqlContext = new SQLContext(sparkContext)

    val file = "/Users/cohenma/Downloads/644139549492-aws-billing-detailed-line-items-with-resources-and-tags-2014-11.csv"
    val header = scala.io.Source.fromFile(file).getLines().take(1).next()
    println(header)
    val data: RDD[String] = sparkContext.textFile(file, 2).filter(line => line != header)


    val schema =
      StructType(header.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    val startQuote = """^\"""".r
    val endQuote = """\"$""".r

    val rowRDD = data.map(_.split(","))
        .map(fields => fields.foreach(startQuote.replaceAllIn(_, "")))
        .map(p => Row.fromSeq(p))

    val gbRDD = data.map(_.split(",")).map(p => Row.fromSeq(Seq(p(20), p(16))))

    //println(rowRDD.toDebugString)
    //val awsSchemaRDD = sqlContext.applySchema(rowRDD, schema)
    val awsSchemaRDD = sqlContext.applySchema(gbRDD,
      StructType(Seq(StructField("ResourceId", StringType, true), StructField("UsageQuantity", StringType, true))))
    awsSchemaRDD.registerTempTable("billingStats")

    var sql: String =
      """
        SELECT ResourceId, sum(UsageQuantity) as TotalUsage
        FROM billingStats
        group by ResourceId
        order by TotalUsage desc
      """

    //sql = "select * from billingStats limit 10"
    //sql = "select ResourceId, UsageQuantity from billingStats limit 10"
    var results:SchemaRDD = sqlContext.sql(sql)

    results.map(t => "Name: " + t(0) + "," + t(1)).xcollect().foreach(println)
   //results.map(r => printRow(r)).collect().foreach(println)
  }

}
