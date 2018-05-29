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
  def svyCount(weighted: Boolean = true) : Double = weighted match {
    case true => df.agg(sum(pweight)).head().getDouble(0)
    case false => df.count().toDouble  
  }
  def svyTotal(est: Column) : SurveyStat
  def svyMean(est: Column) : SurveyStat
  //def svyQuantile(est: Column*, quantile: Double = 0.5) : SurveyStat
  def svyRatio(numerator: Column, denominator: Column) : SurveyStat
  //def svyFreq(est: Column*) : SurveyStat
  
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
  pweight: Column,
  strata: Option[Column] = None,
  cluster: Option[Column] = None,
  fpc: Double = 0
) extends SurveyDesign {
  override def degf: Int = {
    // this is not the scala-ish way to do things
    if (strata.isEmpty && cluster.isEmpty) {
      df.count().toInt - 1
    } else if (strata.isEmpty) {
      cluster match {
        case(Some(cluster)) => df.select(cluster).distinct().count().toInt - 1
      }
    } else if (cluster.isEmpty) {
      strata match {
        case(Some(strata)) => df.count().toInt - df.select(strata).distinct().count().toInt
      }
    } else {
      (cluster, strata) match {
        case(Some(cluster),Some(strata)) => df.groupBy(strata).agg(countDistinct(cluster).alias("total")).agg(sum("total")).head().getLong(0).toInt - df.select(strata).distinct().count().toInt
      }
    }
  }
  override def svyTotal(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg(sum(est * pweight).alias("total")),
    variance = {
    // this is not the scala-ish way to do things
    if (strata.isEmpty && cluster.isEmpty) {
      // need to add fpc
      df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).withColumn("total", est * pweight).agg((count(est) * (1 - fpc) * var_samp("total")).alias("variance"))
    } else if (strata.isEmpty) {
      cluster match {
        case(Some(cluster)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).groupBy(cluster).agg(sum(est * pweight).alias("total"))
        .agg((count("total") * (1 - fpc) * var_samp("total")).alias("variance"))
      }
    } else if (cluster.isEmpty) {
      strata match {
        case(Some(strata)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).withColumn("total", est * pweight).groupBy(strata).agg((count(est) * (1 - fpc) * var_samp("total")).alias("variance"))
        .agg(sum("variance").alias("variance"))
      }
    } else {
      (cluster, strata) match {
        case(Some(cluster), Some(strata)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).groupBy(strata, cluster).agg(sum(est * pweight).alias("total")).
        groupBy(strata).agg((count("total") * (1 - fpc) * var_samp("total")).alias("vari")).agg(sum("vari").alias("variance"))
      }
    }
  },
  statistic = col("total")
  )
  override def svyMean(est: Column) : SurveyStat = new SurveyStat(
    estimate = df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg((sum(est * pweight)/sum(pweight)).alias("mean")),
    variance = {
    // this is not the scala-ish way to do things
    if (strata.isEmpty && cluster.isEmpty) {
      // need to add fpc
      df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).
        agg((sum(pweight * (est - (df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
    } else if (strata.isEmpty) {
      cluster match {
        case(Some(cluster)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).
        groupBy(cluster).agg((sum(pweight * (est - (df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
      }
    } else if (cluster.isEmpty) {
      strata match {
        case(Some(strata)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).
        groupBy(strata).agg((sum(pweight * (est - (df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
      }
    } else {
      (cluster, strata) match {
        case(Some(cluster), Some(strata)) => df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).
        groupBy(strata, cluster).agg((sum(pweight * (est - (df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg((sum(est * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.withColumn(pweight.toString(), when(est.isNotNull, pweight).otherwise(0)).withColumn(est.toString(), when(est.isNotNull, est).otherwise(0)).agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        groupBy(strata).agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("vari")).agg(sum("vari").alias("variance"))
      }
    }
  },
    statistic = col("mean")
  )
  override def svyRatio(numerator: Column, denominator: Column) : SurveyStat = new SurveyStat(
    estimate = df.withColumn(pweight.toString(), when(numerator.isNotNull && denominator.isNotNull, pweight).otherwise(0)).
    withColumn(numerator.toString(), when(numerator.isNotNull, numerator).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
    agg((sum(numerator * pweight)/sum(denominator * pweight)).alias("ratio")),
    variance = {
    // this is not the scala-ish way to do things
    if (strata.isEmpty && cluster.isEmpty) {
      // need to add fpc
      df.withColumn(pweight.toString(), when(numerator.isNotNull && denominator.isNotNull, pweight).otherwise(0)).
    withColumn(numerator.toString(), when(numerator.isNotNull, numerator).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).agg(var_samp(numerator * pweight)/sum(pweight).alias("variance"))
    } else if (strata.isEmpty) {
      cluster match {
        case(Some(cluster)) => df.withColumn(pweight.toString(), when(numerator.isNotNull && denominator.isNotNull, pweight).otherwise(0)).
    withColumn(numerator.toString(), when(numerator.isNotNull, numerator).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(cluster).agg((sum(pweight * (numerator - (df.agg((sum(numerator * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("variance"))
      }
    } else if (cluster.isEmpty) {
      strata match {
        case(Some(strata)) => df.withColumn(pweight.toString(), when(numerator.isNotNull && denominator.isNotNull, pweight).otherwise(0)).
    withColumn(numerator.toString(), when(numerator.isNotNull, numerator).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(strata).agg((count("numerator") * var_samp(pweight * (numerator - (df.agg((sum(numerator * pweight)/sum(pweight)).alias("estimate"))
          .select("estimate").head().getDouble(0))))/(df.agg(sum(pweight).alias("total")).head().getDouble(0))).alias("mean")).
        agg(sum("vari").alias("variance"))
      }
    } else {
      (cluster, strata) match {
        case(Some(cluster), Some(strata)) => df.withColumn(pweight.toString(), when(numerator.isNotNull && denominator.isNotNull, pweight).otherwise(0)).
    withColumn(numerator.toString(), when(numerator.isNotNull, numerator).otherwise(0)).
    withColumn(denominator.toString(), when(denominator.isNotNull, denominator).otherwise(0)).
        groupBy(strata, cluster).agg((sum(pweight * (numerator - (denominator * df.agg((sum(numerator * pweight)/sum(denominator * pweight)).alias("ratio"))
          .select("ratio").head().getDouble(0))))/(df.agg(sum(pweight * denominator).alias("total")).head().getDouble(0))).alias("mean")).
        groupBy(strata).agg((count("mean") * (1 - fpc) * var_samp("mean")).alias("vari")).agg(sum("vari").alias("variance"))
      }
    }
  },
    statistic = col("ratio")
  )
  override def svySubset(bool: Column) : TsDesign = {
    TsDesign(df.withColumn(pweight.toString(), 
                when(bool, pweight).otherwise(0)),
                pweight, strata, cluster, fpc)
  }
  /*override def svyFreq(est: Column*) : SurveyStat = new SurveyStat(
    estimate = df.groupBy(est.map(x => x): _*).agg(sum(pweight)).alias("freq"),
    variance = df.groupBy(est.map(x => x): _*).agg(sum(pweight)).alias("freq"),
    statistic = col("freq"),
    variable = est
    )*/
  //override def svyBy(by: Column*) : GroupedTsDesign = {
  //  GroupedTsDesign(df, by toArray, id, pweight, strata, cluster, fpc)
  //}
}

/*case class GroupedTsDesign (
  df: DataFrame,
  group: Array[Column],
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
  def pweight: Column
  def repweights: Array[Column]
  def mse: Boolean
  def scale: Double
  def rscales: Option[Array[Double]]
}

case class BrrDesign (
  df: DataFrame,
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
