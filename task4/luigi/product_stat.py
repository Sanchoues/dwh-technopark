import os
import sys
import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def read_ok_dem_df(spark, ok_dem_path):
    """
    Чтение данных, полученных на предыдущих знаний
    """
    ok_dem_df = spark.read.option("header", "true").option("sep", ";").csv(ok_dem_path)

    return ok_dem_df


def get_selected_cities_id(spark, name_cities_path, cities):
    """
    Имена городов и их id у росстата
    """

    source_rs_cities_id = (
        spark.read.option("header", "false").option("sep", ";").csv(name_cities_path)
    )

    rs_cities_df = source_rs_cities_id.select(
        sf.col("_c0").cast(StringType()).alias("city_name"),
        sf.col("_c1").cast(IntegerType()).alias("city_id"),
    )

    selected_city_id = rs_cities_df.where(rs_cities_df.city_name.isin(cities))
    return selected_city_id


def get_prices_df(spark, prices_path):
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


def get_products_name(spark, products_path):
    """
    Чтение названий продуктов
    """
    source_products_name_df = (
        spark.read.option("header", "false")
        .option("sep", ";")
        .csv(products_path)
    )

    products_name_df = source_products_name_df.select(
        sf.col("_c0").cast(StringType()).alias("product_name"),
        sf.col("_c1").cast(IntegerType()).alias("product_id"),
    )
    return products_name_df


def get_product_stat(prices_df, selected_city_id, products_name_df):
    """
    Финальная таблица
    """

    product_stat_df = (
        prices_df.join(selected_city_id, on="city_id", how="inner")
        .groupBy("city_name")
        .agg(
            sf.min_by("product_id", "price").alias("cheapest_product_id"),
            sf.max_by("product_id", "price").alias("most_expensive_product_id"),
            (sf.max("price") - sf.min("price").alias("cheapest_product_name")).alias(
                "price_difference"
            ),
        )
        .join(
            products_name_df.select(
                sf.col("product_name").alias("cheapest_product_name"),
                sf.col("product_id").alias("cheapest_product_id"),
            ),
            on="cheapest_product_id",
            how="left",
        )
        .join(
            products_name_df.select(
                sf.col("product_name").alias("most_expensive_product_name"),
                sf.col("product_id").alias("most_expensive_product_id"),
            ),
            on="most_expensive_product_id",
            how="left",
        )
        .select(
            sf.col("city_name"),
            sf.col("cheapest_product_name"),
            sf.col("most_expensive_product_name"),
            sf.col("price_difference"),
        )
    )
    return product_stat_df


def main(argv):
    prices_path = argv[1]
    ok_dem_path = argv[2]
    name_cities_path = argv[3]
    products_path = argv[4]
    output_path = argv[5]

    spark = (
        pyspark.sql.SparkSession.builder.getOrCreate()
    )

    ok_dem_df = read_ok_dem_df(spark, ok_dem_path)
    min_avg_age_city = ok_dem_df.agg(
        sf.min_by("city_name", "age_avg").alias("city_name")
    ).first()["city_name"]
    max_avg_age_city = ok_dem_df.agg(
        sf.max_by("city_name", "age_avg").alias("city_name")
    ).first()["city_name"]
    max_men_share_city = ok_dem_df.agg(
        sf.max_by("city_name", "men_share").alias("city_name")
    ).first()["city_name"]
    max_women_share_city = ok_dem_df.agg(
        sf.max_by("city_name", "women_share").alias("city_name")
    ).first()["city_name"]

    selected_city_id = get_selected_cities_id(
        spark,
        name_cities_path,
        [min_avg_age_city, max_avg_age_city, max_men_share_city, max_women_share_city],
    )
    prices_df = get_prices_df(spark, prices_path)
    products_name_df = get_products_name(spark, products_path)
    product_stat_df = get_product_stat(prices_df, selected_city_id, products_name_df)
    (
        product_stat_df.repartition(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", ";")
        .csv(output_path)
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
