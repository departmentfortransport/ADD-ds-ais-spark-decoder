package uk.gov.dft.ais.decode

import org.apache.spark.sql.{DataFrame, SparkSession}

object RawClean {

  /**
    * Remove unused columns that are present from the decoding process, but probably no that interesting.
    * @param spark SparkSession object
    * @param df Spark Dataframe
    * @return Spark Dataframe
    */
  def removeUnused(spark: SparkSession, df: DataFrame): DataFrame = {
    df.drop(
      "dataBinary",
      "data",
      "packetType",
      "fragmentCount",
      "radioChannel",
      "padding",
      "s"
    )
  }

}


