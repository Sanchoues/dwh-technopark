import os
import sys
import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def get_products_for_stat(spark, products_for_stat_path):
    """
    Список продуктов, которые учитываются в статистике
    """

    source_products_for_stat_df = (
        spark.read
        .option("header", "false")
        .csv(products_for_stat_path)
    )

    products_for_stat_df = source_products_for_stat_df.select(
        sf.col("_c0").cast(IntegerType()).alias("product_id"),
    )
    return products_for_stat_df


def get_prices(spark, prices_path):
    """
    Набор данных по всем товарам и ценам
    """

    source_prices_df = (
        spark.read.option("header", "false")
        .option("sep", ";")
        .csv(prices_path)
    )

    source_prices_df = source_prices_df.replace(",", ".", "_c2")

    prices_df = source_prices_df.select(
        sf.col("_c0").cast(IntegerType()).alias("city_id"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
        sf.regexp_replace("_c2", ",", ".").cast(FloatType()).alias("price"),
    )

    return prices_df


def get_price_stat(products_for_stat_df, prices_df):
    """
    Статистика по необходимым товарам
    """

    price_stat_df = (
        prices_df.join(products_for_stat_df, on="product_id", how="left")
        .groupBy("product_id")
        .agg(
            sf.min("price").alias("min_price"),
            sf.max("price").alias("max_price"),
            sf.round(sf.avg("price"), 2).alias("price_avg"),
        )
    )

    return price_stat_df


def main(argv):
    prices_path = argv[1]
    products_for_stat_path = argv[2]
    price_stat_result_path = argv[3]

    spark = (
        pyspark.sql.SparkSession.builder.getOrCreate()
    )

    prices_df = get_prices(spark, prices_path)
    products_for_stat_df = get_products_for_stat(spark, products_for_stat_path)
    price_stat_df = get_price_stat(products_for_stat_df, prices_df)

    (
        price_stat_df.repartition(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", ";")
        .csv(price_stat_result_path)
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
