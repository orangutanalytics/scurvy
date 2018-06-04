package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column

sealed trait SurveyStat {
  def estimate: DataFrame
  def variance: DataFrame
  def statistic: Column
  def SE : DataFrame = variance.withColumn("SE", sqrt("variance")).drop("variance")
  def cv : DataFrame = {
    estimate.withColumn(".temp_id", monotonically_increasing_id()).join(
      SE.withColumn(".temp_id", monotonically_increasing_id()), ".temp_id"
    ).drop(".temp_id").withColumn("cv", col("SE")/statistic).drop("SE", statistic.toString())
  }
  /*deff : DataFrame
  confint(method : String) : Dataframe*/
}

case class SurveyTotal(svy: SurveyDesign, est: Column) extends SurveyStat {
  override def estimate: DataFrame = {
    svy.svyFilterMissing(est).agg(sum(est * svy.pweight).alias("total"))
  }
  override def variance: DataFrame = {
  svy match {
    case TsDesign(df, pweight, strata, cluster, fpc) => (cluster, strata) match {
    case (None, None) => svy.svyFilterMissing(est).agg((count(est) * (1 - fpc) * var_samp(est * svy.pweight)).alias("variance"))
    case (Some(cluster), None) => svy.svyFilterMissing(est).groupBy(cluster).agg(sum(est * svy.pweight).alias("total"))
        .agg((count("total") * (1 - fpc) * var_samp("total")).alias("variance"))
    case (None, Some(strata)) =>  svy.svyFilterMissing(est).groupBy(strata).agg((count(est) * (1 - fpc) * var_samp(est * svy.pweight)).alias("variance"))
        .agg(sum("variance").alias("variance"))
    case (Some(cluster), Some(strata)) => svy.svyFilterMissing(est).groupBy(strata, cluster).agg(sum(est * svy.pweight).alias("total")).
        groupBy(strata).agg((count("total") * (1 - fpc) * var_samp("total")).alias("vari")).agg(sum("vari").alias("variance"))
    case _ => throw new Exception("This should never happen!")
    }
    case _ => throw new Exception("I didn't implement all the survey designs yet sorry!")
  }
  }
  override def statistic: Column = col("total")
}

case class SurveyMean(svy: SurveyDesign, est: Column) extends SurveyStat {
  override def estimate: DataFrame = {
    svy.svyFilterMissing(est).agg((sum(est * svy.pweight)/sum(svy.pweight)).alias("mean"))
  }
  override def variance: DataFrame = {
  svy match {
    case TsDesign(df, pweight, strata, cluster, fpc) => (cluster, strata) match {
    case (None, None) => svy.svyFilterMissing(est).
        agg((sum(pweight * (est - (svy.svyFilterMissing(est).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(est).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
    case (Some(cluster), None) => svy.svyFilterMissing(est).
        groupBy(cluster).agg((sum(pweight * (est - (svy.svyFilterMissing(est).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(est).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
    case (None, Some(strata)) => svy.svyFilterMissing(est).
        groupBy(strata).agg((sum(pweight * (est - (svy.svyFilterMissing(est).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(est).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
    case (Some(cluster), Some(strata)) => svy.svyFilterMissing(est).
        groupBy(strata, cluster).agg((sum(pweight * (est - (svy.svyFilterMissing(est).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(est).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        groupBy(strata).agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("vari")).agg(sum("vari").alias("variance"))
    case _ => throw new Exception("This should never happen!")
    }
    case _ => throw new Exception("I didn't implement all the survey designs yet sorry!")
  }
  }
  override def statistic: Column = col("mean")
}

case class SurveyRatio(svy: SurveyDesign, numerator: Column, denominator: Column) extends SurveyStat {
  override def estimate: DataFrame = {
    svy.svyFilterMissing(numerator).withColumn(svy.pweight.toString(), when(denominator.isNotNull, svy.pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
    agg((sum(numerator * svy.pweight)/sum(denominator * svy.pweight)).alias("ratio"))
  }
  override def variance: DataFrame = {
  svy match {
    case TsDesign(df, pweight, strata, cluster, fpc) => (cluster, strata) match {
    case (None, None) => svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg(var_samp(numerator * pweight)/sum(pweight).alias("variance"))
    case (Some(cluster), None) => svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(cluster).agg((sum(pweight * (numerator - (svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg((sum(numerator * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
    case (None, Some(strata)) => svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(strata).agg((count("numerator") * var_samp(pweight * (numerator - (svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg((sum(numerator * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg(sum("vari").alias("variance"))
    case (Some(cluster), Some(strata)) => svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(strata, cluster).agg((sum(pweight * (numerator - (denominator * svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg((sum(numerator * pweight)/sum(denominator * pweight)).alias("ratio"))
          .select("ratio").head().getDouble(0))))/(svy.svyFilterMissing(numerator).withColumn(pweight.toString(), when(denominator.isNotNull, pweight).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg(sum(pweight * denominator).alias("total")).head().getDouble(0))).alias("mean")).
        groupBy(strata).agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("vari")).agg(sum("vari").alias("variance"))
    case _ => throw new Exception("This should never happen!")    
    }
    case _ => throw new Exception("I didn't implement all the survey designs yet sorry!")
  }
  }
  override def statistic: Column = col("mean")
}

