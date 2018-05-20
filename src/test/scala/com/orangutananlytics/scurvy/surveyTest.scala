package com.orangutanalytics.scurvy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

class SurveyTest extends FunSuite {
    val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
    val R = org.ddahl.rscala.RClient()
    import spark.implicits._
    val nhanes = spark.read.format("csv").option("header","true").option("inferSchema", "true").load("data/nhanes.csv")
    // R Code From Lumley
    // svydesign(id=~SDMVPSU, strata=~SDMVSTRA, weights=~WTMEC2YR, nest=TRUE, data=nhanes)
    val nhanes_survey = new TsDesign(nhanes, $"SDMVPSU", $"WTMEC2YR", Some($"SDMVSTRA"))
    test("A SurveyStat objectis buildable") {

        val test = new SurveyStat(
            estimate = Seq(
                ("A", 24.0)
            ).toDF("level", "total"),
            variance = Seq(
                ("A", 2.5)
            ).toDF("level", "variance")
        )
        assert(test.estimate.select("level").head().getString(0) === "A")
        assert(test.variance.select("level").head().getString(0) === "A")
        assert(test.estimate.select("total").head().getDouble(0) === 24)
        assert(test.variance.select("variance").head().getDouble(0) === 2.5)
    }
    
    test("Survey Designs Degrees of Freedom") {
      assert(nhanes_survey.degf === 16)
    }
    test("svyCount works for taylor series") {
      assert(nhanes_survey.svyCount(false) === 8591)
      assert(nhanes_survey.svyCount() === 276536446)
    }
    
      (nhanes_survey.svyTotal(col("HI_CHOL"))).estimate.show()
      (nhanes_survey.svyBy(col("race")).svyTotal(col("HI_CHOL"))).estimate.show()
    }
}
