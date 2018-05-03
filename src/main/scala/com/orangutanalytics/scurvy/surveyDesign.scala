package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
 
sealed trait SurveyDesign {
  def df: DataFrame
  def id: Column
  def pweight: Column
  def svySubset(bool: Column) : SurveyDesign
  // weighted total of continuous variable
  def svyTotal(est: Column) : SurveyStat
  
  // weighted mean of continuous variable
  def svyMean(est: Column) : SurveyStat

}

sealed trait RegularSurveyDesign extends SurveyDesign {
  def df: DataFrame
  def id: Column
  def pweight: Column
  def svyTotal(est: Column) : SurveyStat
  
  // weighted mean of continuous variable
  def svyMean(est: Column) : SurveyStat
  
    // subset
  def svySubset(bool: Column) : RegularSurveyDesign 
  // by
  // return grouped survey design object
  def svyBy(by: Column*) : GroupedSurveyDesign
}

sealed trait GroupedSurveyDesign extends SurveyDesign {
  def df: DataFrame
  def group: Array[Column]
  def id: Column
  def pweight: Column
  def svyTotal(est: Column) : SurveyStat = new SurveyStat(
    estimate = {
      df.groupBy(group.filter(_ != None).map(x => x): _*).agg((sum(est * pweight)).alias("total"))
    },
    variance = {
      df.groupBy(group.filter(_ != None).map(x => x): _*).agg((sum(est * pweight)).alias("total"))
    }
  )
  def svySubset(bool: Column) : GroupedSurveyDesign
  
  // weighted mean of continuous variable
  override def svyMean(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.groupBy(group.filter(_ != None).map(x => x): _*).agg((sum(est * pweight)/sum(pweight)).alias("mean")),
    variance = df.groupBy(group.filter(_ != None).map(x => x): _*).agg((sum(est * pweight)/sum(pweight)).alias("mean"))
  )

}

case class TsDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  strata: Option[Column] = None,
  cluster: Option[Column] = None,
  fpc: Double = 0
) extends RegularSurveyDesign {
  override def svyTotal(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.agg(sum(est * pweight).alias("total")),
    variance = df.agg(sum(est * pweight).alias("total"))
  )
  override def svyMean(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.agg((sum(est * pweight)/sum(pweight)).alias("total")),
    variance = df.agg((sum(est * pweight)/sum(pweight)).alias("total"))
  )
  override def svySubset(bool: Column) : TsDesign = {
    TsDesign(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, strata, cluster, fpc)
  }
  override def svyBy(by: Column*) : GroupedTsDesign = {
    GroupedTsDesign(df, by toArray, id, pweight, strata, cluster, fpc)
  }
}

case class GroupedTsDesign (
  df: DataFrame,
  group: Array[Column],
  id: Column,
  pweight: Column,
  strata: Option[Column] = None,
  cluster: Option[Column] = None,
  fpc: Double = 0
) extends GroupedSurveyDesign {
  override def svySubset(bool: Column) : GroupedTsDesign = {
    GroupedTsDesign(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                group, id, pweight, strata, cluster, fpc)
  }
}

sealed trait RegularReplicateDesign extends RegularSurveyDesign {
  def df: DataFrame
  def id: Column
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
}

sealed trait GroupedReplicateDesign extends RegularSurveyDesign {
  def df: DataFrame
  def group: Array[Column]
  def id: Column
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
}
/*
case class BrrDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  repweights: Array[Column],
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None
) extends Regularreplicate_design {
  override def svy_subset(bool: Column) : BRR_design = {
    BRR_design(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, repweights, mse, scale, rscales)
  }
  override def svy_by(by: Column*) : GroupedBRR_design = {
    GroupedBRR_design(df.groupBy(by.map(x => x): _*), id, pweight, strata, cluster, fpc)
  }
}

case class FayDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  repweights: Array[Column],
  rho: Double,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None
) extends replicate_design {
  override def svy_subset(bool: Column) : Fay_design = {
    Fay_design(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, repweights, rho, mse, scale, rscales)
  }
}

case class Jk1Design (
  df: DataFrame,
  id: Column,
  pweight: Column,
  repweights: Array[Column],
  fpc: Double = 0,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None
) extends replicate_design {
  override def svy_subset(bool: Column) : JK1_design = {
    JK1_design(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, repweights, fpc, mse, scale, rscales)
  }
}

case class JknDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  repweights: Array[Column],
  fpc: Double = 0,
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None
) extends replicate_design {
  override def svy_subset(bool: Column) : JKn_design = {
    JKn_design(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, repweights, fpc, mse, scale, rscales)
  }
}

case class BootstrapDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  repweights: Array[Column],
  mse: Boolean = true,
  scale: Double = 1,
  rscales: Option[Array[Double]] = None
) extends replicate_design {
  override def svy_subset(bool: Column) : Bootstrap_design = {
    Bootstrap_design(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                id, pweight, repweights, mse, scale, rscales)
  }
}
*/
