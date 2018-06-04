package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

sealed trait SurveyDesign {
  // basic attributes
  def df: DataFrame
  def pweight: Column
  def degf: Int
  // summary statistics
  def svyCount(weighted: Boolean = true): Double = weighted match {
    case true => df.agg(sum(pweight)).head().getDouble(0)
    case false => df.count().toDouble
  }
  // transformations
  def svySubset(bool: Column): SurveyDesign
  def svyFilterMissing(est: Column): DataFrame = {
    df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0))
  }

}

case class TsDesign(
  df: DataFrame,
  pweight: Column,
  strata: Option[Column] = None,
  cluster: Option[Column] = None,
  fpc: Double = 0) extends SurveyDesign {
  override def degf: Int = (cluster, strata) match {
    case (None, None) => df.count().toInt - 1
    case (Some(cluster), None) => df.select(cluster).distinct().count().toInt - 1
    case (None, Some(strata)) => df.count().toInt - df.select(strata).distinct().count().toInt
    case (Some(cluster), Some(strata)) => df.groupBy(strata).agg(countDistinct(cluster).alias("total")).agg(sum("total")).head().getLong(0).toInt - df.select(strata).distinct().count().toInt
  }

  override def svySubset(bool: Column): TsDesign = {
    TsDesign(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, strata, cluster, fpc)
  }
}

sealed trait ReplicateDesign extends SurveyDesign {
  def df: DataFrame
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
  def degf: Int = repweights.length
}

case class BrrDesign(
  df: DataFrame,
  pweight: Column,
  repweights: Array[Column],
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None) extends ReplicateDesign {
  override def svySubset(bool: Column): BrrDesign = {
    BrrDesign(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, repweights, mse, scale, rscales)
  }
}

case class FayDesign(
  df: DataFrame,
  pweight: Column,
  repweights: Array[Column],
  rho: Double,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None) extends ReplicateDesign {
  override def svySubset(bool: Column): FayDesign = {
    FayDesign(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, repweights, rho, mse, scale, rscales)
  }
}

case class Jk1Design(
  df: DataFrame,
  pweight: Column,
  repweights: Array[Column],
  fpc: Double = 0,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None) extends ReplicateDesign {
  override def svySubset(bool: Column): Jk1Design = {
    Jk1Design(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, repweights, fpc, mse, scale, rscales)
  }
}

case class JknDesign(
  df: DataFrame,
  pweight: Column,
  repweights: Array[Column],
  fpc: Double = 0,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None) extends ReplicateDesign {
  override def svySubset(bool: Column): JknDesign = {
    JknDesign(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, repweights, fpc, mse, scale, rscales)
  }
}

case class BootstrapDesign(
  df: DataFrame,
  pweight: Column,
  repweights: Array[Column],
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None) extends ReplicateDesign {
  override def svySubset(bool: Column): BootstrapDesign = {
    BootstrapDesign(
      df.withColumn(
      pweight.toString(),
      when(bool, pweight).otherwise(0)),
      pweight, repweights, mse, scale, rscales)
  }
}

