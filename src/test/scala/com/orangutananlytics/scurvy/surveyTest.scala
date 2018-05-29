package com.orangutanalytics.scurvy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SurveyTest extends FunSuite {
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val R = org.ddahl.rscala.RClient()
    import spark.implicits._
    test("A SurveyStat objectis buildable") {

        val test = new SurveyStat(
            estimate = Seq(
                ("A", 24.0)
            ).toDF("level", "total"),
            variance = Seq(
                ("A", 2.5)
            ).toDF("level", "variance"),
            statistic = col("total")
        )
        assert(test.estimate.select("level").head().getString(0) === "A")
        assert(test.variance.select("level").head().getString(0) === "A")
        assert(test.estimate.select("total").head().getDouble(0) === 24)
        assert(test.variance.select("variance").head().getDouble(0) === 2.5)
    }
    
    val nhanes = spark.read.format("csv").option("header","true").option("inferSchema", "true").option("nullValue", "NA").load("data/nhanes.csv").cache()
    // R Code From Lumley
    // svydesign(id=~SDMVPSU, strata=~SDMVSTRA, weights=~WTMEC2YR, nest=TRUE, data=nhanes)
    val nhanes_cluster_strata = new TsDesign(nhanes, $"WTMEC2YR", Some($"SDMVSTRA"), Some($"SDMVPSU"))
    val nhanes_cluster = new TsDesign(nhanes, $"WTMEC2YR", None, Some($"SDMVPSU"))
    val nhanes_strata = new TsDesign(nhanes, $"WTMEC2YR", Some($"SDMVSTRA"), None)
    val nhanes_rs = new TsDesign(nhanes, $"WTMEC2YR", None, None)
    
    test("count") {
      assert(nhanes_cluster_strata.svyCount(false) === 8591)
      assert(Math.round(nhanes_cluster_strata.svyCount()) === 276536446)
    }
    
    test("degrees of freedom") {
      assert(nhanes_cluster_strata.degf === 16)
      assert(nhanes_cluster.degf === 2)
      assert(nhanes_strata.degf === 8576)
      assert(nhanes_rs.degf === 8590)
    }
    
    test("subset") {
      val nhanes_hispanic = nhanes_cluster_strata.svySubset($"race" === 1)
      assert(nhanes_hispanic.svyCount(false) === 8591)
      assert(Math.round(nhanes_hispanic.svyCount()) === 41633252)
    }
    
    test("svytotal") {
      assert(nhanes_cluster_strata.svyTotal($"HI_CHOL").estimate.select("total").head().getDouble(0).toInt === 28635245)
      // numerical issue so just checking we are close
      assert(Math.abs(nhanes_cluster_strata.svyTotal($"HI_CHOL").SE.select("SE").head().getDouble(0).toInt - 2020711) <= 1)
      assert(Math.abs(nhanes_cluster_strata.svyTotal($"HI_CHOL").cv.select("cv").head().getDouble(0) - 0.07056726) <= 0.001)
      
      assert(Math.abs(nhanes_cluster.svyTotal($"HI_CHOL").SE.select("SE").head().getDouble(0).toInt - 13767540) <= 1)
      assert(Math.abs(nhanes_rs.svyTotal($"HI_CHOL").SE.select("SE").head().getDouble(0).toInt - 1244260) <= 1)
      assert(Math.abs(nhanes_strata.svyTotal($"HI_CHOL").SE.select("SE").head().getDouble(0).toInt - 1240406) <= 1)
    }
    
    test("svymean") {
      assert(Math.abs(nhanes_cluster_strata.svyMean($"HI_CHOL").estimate.select("mean").head().getDouble(0) - 0.11214) <= 0.00001)
      // numerical issue so just checking we are close
      assert(Math.abs(nhanes_cluster_strata.svyMean($"HI_CHOL").variance.select("variance").head().getDouble(0) - 0.00002965717) <= 0.0000000001)
      assert(Math.abs(nhanes_cluster_strata.svyMean($"HI_CHOL").cv.select("cv").head().getDouble(0) - 0.04856158) <= 0.00000001)
    }
    
    test("svyratio") {
      assert(Math.abs(nhanes_cluster_strata.svySubset(col("HI_CHOL").isNotNull && col("race").isNotNull).svyRatio($"HI_CHOL", $"race").estimate.select("ratio").head().getDouble(0) - 0.05331284) <= 0.0000001)
      assert(Math.abs(nhanes_cluster_strata.svySubset(col("HI_CHOL").isNotNull && col("race").isNotNull).svyRatio($"HI_CHOL", $"race").SE.select("SE").head().getDouble(0) - 0.002867244) <= 0.00000001)
    }

}
