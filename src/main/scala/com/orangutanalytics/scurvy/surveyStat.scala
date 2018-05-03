package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame

case class SurveyStat (
  estimate: DataFrame,
  variance: DataFrame
)
