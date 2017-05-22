

import feature_generation.Feature_generation
import utils.SparkSpec

import org.scalatest.{FlatSpec, GivenWhenThen, Matchers}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.scalatest.{FlatSpec}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.{Vector, Vectors, SparseVector}
import org.apache.spark.ml.{Pipeline, PipelineModel}

class Feature_generatorTestSuite extends FlatSpec with SparkSpec with Matchers {

  it should "replace the expected null values in a dataframe " in {

    val mtcars = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").option("inferSchema", "true").load("src/test/resources/mtcars.csv")

    val expectedOutome = 32
    val test_val = mtcars.count
    test_val should equal (expectedOutome)

  }

  it should "calculate VIF for the expected columns and find the expected offender" in {

    val mtcars = spark.read.format(spark,"com.databricks.spark.csv").option("header", "true").option("delimiter",",").option("inferSchema", "true").load("src/test/resources/mtcars.csv")

    val expectedLength = 5
    val coVarThresh = 9.0
    val test_val = Feature_generation.calculateVIF(mtcars)
    val expectedOffender = "wt"
    val foundOffender = test_val.filter(x => x._2 > coVarThresh).map(z => z._1).mkString(",")
    test_val.toArray.size should equal (expectedLength)
    foundOffender should equal (expectedOffender)
  }


  it should "should do one-hot as expected " in {

    val mtcars = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter",",").option("inferSchema", "true").load("src/test/resources/mtcars.csv")

    val expectedOutome = 32
    val test_val = mtcars.count
    test_val should equal (expectedOutome)


    val in_df = spark.createDataFrame(Seq(
      ("id_0",0.3, Array("cat0", "cat2")),
      ("id_1",0.2,Array("cat0", "cat1")),
      ("id_2",0D,Array("cat2")),
      ("id_3",3D, Array("cat0", "cat1", "cat3", "cat4", "cat5"))
    )).toDF("id","ph_value","categories")

    val lookup_df = in_df.select("categories").rdd.map{row =>
      row.getAs[Seq[String]](0).toArray
    }
    val value_freq = lookup_df.flatMap(x => x).map(x => (x.trim,1)).reduceByKey((a,b) => a+b).map(_.swap).sortByKey(false)
    val lookup_map =  Map("cat0" -> 0, "cat1" -> 1, "cat2" -> 2, "cat3" -> 3, "cat4" -> 4)

    val oneHotted_test = stringArrayFeatureToOneHotVectorTrunc(in_df, Seq("categories"), lookup_map, "categories_oneHot", 3, Seq("id"))

    val expectedOutcome  = sc.parallelize(Seq(
      ("id_0",2, Array(1D, 0D, 1D, 0D)),
      ("id_1",2, Array(1D, 1D, 0D, 0D)),
      ("id_2",1, Array(0D, 0D, 1D, 0D)),
      ("id_3",5, Array(1D, 1D, 0D, 1D))
    )).collect

    oneHotted_test.collect should equal(expectedOutcome)

  }


}
