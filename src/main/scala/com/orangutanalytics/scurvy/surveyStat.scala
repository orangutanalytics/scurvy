package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

case class SurveyStat (
  estimate: DataFrame,
  variance: DataFrame,
  statistic: Column,
  variable: Column
) {
  def SE : DataFrame = variance.withColumn("SE", sqrt("variance")).drop("variance")
  def cv : DataFrame = {
    estimate.withColumn(".temp_id", monotonically_increasing_id()).join(
      SE.withColumn(".temp_id", monotonically_increasing_id()), ".temp_id"
    ).drop(".temp_id").withColumn("cv", col("SE")/statistic).drop("SE", statistic.toString())
  }
  /*deff : DataFrame
  confint(method : String) : Dataframe*/
}
