package feature_generation

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.regression.LinearRegression
import scala.collection.mutable
import spark.implicits._

object Feature_generation {

  def featureListNegativeSelect(feature_names: Array[String], holdOut: String) = {
    feature_names.diff(List(holdOut))
  }

  /*
    Warn: Work in progress:
  */
  def calculateVIF(df: DataFrame) = {
    val features = df.dtypes.filter(x => x._2 == "DoubleType").map(z => z._1)
    val numIterations = 15
    val algorithm = new LinearRegression()
    algorithm.setFitIntercept(true)
    algorithm.setMaxIter(numIterations)

    var VIF = ArrayBuffer[(String, Double)]()
    for (i <- features) {
      val X_i = i.toString
      val X_rest = featureListNegativeSelect(features, i)
      val assembler = new VectorAssembler()
      assembler.setInputCols(X_rest)
      assembler.setOutputCol("feat_" + i)
      algorithm.setPredictionCol("vif_" + i)
      algorithm.setLabelCol(X_i)
      algorithm.setFeaturesCol("feat_" + i)
      val loop_df = assembler.transform(df)
      val model = algorithm.fit(loop_df)
      val loop_predictions = model.transform(loop_df)
      val mergedCols = Seq(X_i) ++ Seq("vif_"+i.toString)
      val valuesAndPreds = loop_predictions.select(mergedCols.head, mergedCols.tail: _*).rdd.map { row =>
        (row.getDouble(0),row.getDouble(1))
      }
      val SS_res = valuesAndPreds.map { case (v, p) => math.pow((v - p), 2) }.sum()
      val yhat = valuesAndPreds.map { case (v, p) => v }.mean()
      val SS_tot = valuesAndPreds.map { case (v, p) => math.pow((v - yhat), 2) }.sum()
      val Rsq = 1 - SS_res / SS_tot
      val loop_vif = (1 / (1 - Rsq)).toDouble
      VIF += ((X_i, loop_vif))
    }
    VIF

  }

  def stringArrayFeatureToOneHotVectorTrunc(sprk_sesh: org.apache.spark.sql.SparkSession,ogDataFrame: DataFrame, colsToOneHotEncode: Seq[String], oneHotValueMap: Map[String,Int], oneHotName: String, max_num: Int, pk_cols: Seq[String]) = {
    import sprk_sesh.implicits._
    val mergedCols = (pk_cols ++ colsToOneHotEncode)
    val indicesToEncode = pk_cols.size to mergedCols.size
    val oneHotValueMap_mut = mutable.Map.empty ++= oneHotValueMap.map(_.swap)
    val lookup_map = if (oneHotValueMap.size <= max_num) oneHotValueMap  else oneHotValueMap_mut.retain((k,v) => k < max_num) += (max_num -> "Rest")
    val oneHottie = ogDataFrame.select(mergedCols.head, mergedCols.tail: _*).rdd.map { row =>
      val id_1 = row.getString(0)
      val techToEncode = row.getAs[Seq[String]](1).map(x => x.trim).distinct.toSet
      val arrayToEncode = techToEncode.toList.map { f =>
        Math.min(oneHotValueMap.get(f).get,max_num)
      }.distinct.toArray

      val numberOfElems = techToEncode.size.toDouble
      val oneHotVect = new org.apache.spark.ml.linalg.SparseVector(lookup_map.size, arrayToEncode, arrayToEncode.map(x => 1.00.toDouble)).toDense
      (id_1, numberOfElems, oneHotVect)
    }
    val output_cols = Seq(pk_cols(0)) ++ Seq(colsToOneHotEncode(0)+"_cnt") ++ Seq(oneHotName)
    oneHottie.toDF(output_cols: _*)
  }

}
