package com.orangutanalytics.scurvy

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
 
sealed trait SurveyDesign {
  // basic attributes
  def df: DataFrame
  def id: Column
  def pweight: Column
  def degf: Int
  // summary statistics
  def svyCount(weighted: Boolean = true) : Double = weighted match {
    case true => df.agg(sum(pweight)).head().getDouble(0)
    case false => df.count().toDouble  
  }
  def svyTotal(est: Column*) : SurveyStat
  def svyQuantile(est: Column*, quantile: Double = 0.5) : SurveyStat
  //def svyRatio(est: Column) : SurveyStat
  def svyFreq(est: Column*) : SurveyStat
  
  // models
  //def svyGlm() : SurveyModelStat

  // transformations
  def svySubset(bool: Column) : SurveyDesign
  //def svyGroupBy(by: Column*) : GroupedSurveyDesign
  //def svyCalibrate() : SurveyDesign
  //def svyPostStratify() : SurveyDesign
  //def svyRake() : SurveyDesign
  

}

/*sealed trait GroupedSurveyDesign extends SurveyDesign {
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
}*/

case class TsDesign (
  df: DataFrame,
  id: Column,
  pweight: Column,
  strata: Option[Column] = None,
  cluster: Option[Column] = None,
  fpc: Double = 0,
  degf: Int = {
    if (strata.isEmpty && cluster.isEmpty) {
      df.count().toInt() - 1
    } else if (strata.isEmpty) {
      df.distinct(cluster).count().toInt() - 1
    } else if (cluster.isEmpty) {
      df.count().toInt - df.distinct(strata).count().toInt
    } else {
      df.distinct(cluster).count().toInt - df.distinct(strata).count().toInt
    }
  }
) extends SurveyDesign {
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
  override def svyFreq(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.groupBy(est).agg(sum(pweight)).alias("freq"),
    variance = df.groupBy(est).agg(sum(pweight)).alias("freq")
  )
  //override def svyBy(by: Column*) : GroupedTsDesign = {
  //  GroupedTsDesign(df, by toArray, id, pweight, strata, cluster, fpc)
  //}
}

/*case class GroupedTsDesign (
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
}*/

sealed trait RegularReplicateDesign extends SurveyDesign {
  def df: DataFrame
  def id: Column
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
  def degf: Int = repweights.length
}

/*sealed trait GroupedReplicateDesign extends SurveyDesign {
  def df: DataFrame
  def group: Array[Column]
  def id: Column
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
}

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
