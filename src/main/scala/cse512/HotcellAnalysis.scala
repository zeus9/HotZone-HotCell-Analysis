package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  /**
   * Analyze hot cells of the given point data. This method calculates the Getis-Ord statistic.
   *
   * @param spark     a spark session
   * @param pointPath the path to a data set of points
   * @return the result in a data frame
   */
  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath)
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points

    spark.udf.register("CalculateX", (pickupPoint: String) => {
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    })
    spark.udf.register("CalculateY", (pickupPoint: String) => {
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    })
    spark.udf.register("CalculateZ", (pickupTime: String) => {
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
    }) // Z = day

    pickupInfo = spark.sql(
      "SELECT CalculateX(nyctaxitrips._c5), CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) " +
        "FROM nyctaxitrips")

    val newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    println("Raw pickupInfo")
    pickupInfo.show()

    // Define the min and max of x, y, z
    //
    // Quoted from the instruction:
    // https://github.com/jiayuasu/CSE512-Project-Hotspot-Analysis-Template
    //
    // "You may clip the source data to an envelope (latitude 40.5N – 40.9N,
    // longitude 73.7W – 74.25W) encompassing the New York City in order to
    // remove some of the noisy error data."
    //
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // Clip the cell data and concat point coordinates together
    pickupInfo = pickupInfo
      .filter(s"($minX <= x) AND (x <= $maxX) AND ($minY <= y) AND (y <= $maxY) AND ($minZ <= z) AND (z <= $maxZ)")
      .select(concat_ws(":", col("x"), col("y"), col("z")))
      .withColumnRenamed("concat_ws(:, x, y, z)", "point")
    // Update the temp view
    pickupInfo.createOrReplaceTempView("pickupInfo")
    println("Clipped pickupInfo")
    pickupInfo.show()

    // Find cells in the neighborhood
    spark.udf.register("FindNeighborCells", (p1Str: String, p2Str: String) => {
      HotcellUtils.FindNeighborCells(p1Str, p2Str)
    })
    val dfNeighbors = spark.sql(
      "SELECT p1.point p1, p2.point p2 " +
        "FROM pickupInfo p1, pickupInfo p2 " +
        "WHERE FindNeighborCells(p1.point, p2.point) AND p1.point != p2.point")
    dfNeighbors.createOrReplaceTempView("neighbors")
    println("The Neighbor Table")
    dfNeighbors.show(10)

    // Compute the "attribute"
    spark.udf.register("ST_Within", (cellCoor: String, rawPointCoor: String, rawPointDate: String) => {
      HotcellUtils.ST_Within(cellCoor, rawPointCoor, rawPointDate)
    })
    var dfAttrs = spark.sql(
      "SELECT cell.point point, ntt._c5, ntt._c1 " +
        "FROM pickupInfo cell, nyctaxitrips ntt " +
        "WHERE ST_Within(cell.point, ntt._c5, ntt._c1)")
    dfAttrs = dfAttrs.groupBy("point").count().withColumnRenamed("count", "attributeVal")
    dfAttrs.createOrReplaceTempView("attributes")
    println("The Attribute Table")
    dfAttrs.show(10)

    // Join the attributes and neighbors
    val dfNeighborAttrs = spark.sql(
      "SELECT N.p1, N.p2, A.attributeVal " +
        "FROM neighbors N, attributes A " +
        "WHERE N.p2 = A.point")
    println("The Neighbor-Attribute Table")
    dfNeighborAttrs.show(10)

    // Compute the G scores (by assuming that the weight equals to 1 for all neighbors and 0 the otherwise)
    // - compute population statistics first
    // - then aggregate neighbors

    val valXBar = dfAttrs.select(sum("attributeVal") / numCells).first().getDouble(0)
    val nume1 = sum("attributeVal")
    val nume2 = count("*") * valXBar
    //    val valStd = dfAttrs.select((sum(pow("attributeVal", 2)) / numCells) - math.pow(valXBar, 2)).first().getDouble(0)
    val denomeInner1 = count("*") * numCells
    val denomeInner2 = pow(count("*"), 2)
    //    val denome1 = sqrt((denomeInner1 - denomeInner2) / (numCells - 1)) * valStd
    val denome1 = (denomeInner1 - denomeInner2) / (numCells - 1)
    val gscores = dfNeighborAttrs.groupBy("p1").agg((nume1 - nume2) / denome1).toDF(Seq("p1", "gscore"): _*)

    val splitPoint = split(gscores.col("p1"), ":")
    val result = gscores
      .withColumn("x", splitPoint.getItem(0))
      .withColumn("y", splitPoint.getItem(1))
      .withColumn("z", splitPoint.getItem(2))
      .select("x", "y", "z")
      //      .select("x", "y", "z", "gscore")
      .orderBy(desc("gscore"))
    result.show()

    result
  }
}
