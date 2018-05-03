package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame

case class SurveyStat (
  estimate: DataFrame,
  variance: DataFrame
)

import org.apache.spark.sql.SparkSession

object SparkApp2 {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    import spark.implicits._
    val test = new SurveyStat(
      estimate = Seq(
  ("A", 24)
).toDF("level", "total"),
    variance = Seq(
  ("A", 2.5)
).toDF("level", "SE")
    )
    test.estimate.show()
    test.variance.show()
  }
}
