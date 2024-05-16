import os
import sys
import pyspark
from pyspark.sql.types import *
import pyspark.sql.functions as sf


def get_cities_df(spark, price_stat_path, prices_path):
    """
    Города, где хотя бы на один товар цена выше среднего
    """

    # Цены на товар (задано)
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

    # Статистика по товарами (предыдущая задача)
    price_stat_df = (
        spark.read.option("header", "true")
        .option("sep", ";")
        .csv(price_stat_path)
    )

    cities_df = (
        prices_df.groupBy(["city_id", "product_id"])
        .agg(sf.avg("price").alias("price_avg"))
        .join(
            price_stat_df.select(
                sf.col("price_avg").alias("price_avg_by_all_cities"),
                sf.col("product_id"),
            ),
            on="product_id",
            how="left",
        )
        .select("city_id")
        .where(sf.col("price_avg") > sf.col("price_avg_by_all_cities"))
        .distinct()
    )

    return cities_df


def get_russia_id(spark, country_path):
    """
    ID России
    """
    source_countries_df = (
        spark.read.option("header", "false")
        .option("sep", ",")
        .csv(country_path)
    )

    countries_df = source_countries_df.select(
        sf.col("_c0").cast(StringType()).alias("country_id"),
        sf.col("_c1").cast(StringType()).alias("name"),
    )

    russia_id = countries_df.where(countries_df.name == "Россия").select("country_id")
    return russia_id


def get_ok_rs_cities_id(spark, cities_path):
    """
    Сопоставление id росстата и ok
    """
    source_ok_rs_cities_id = (
        spark.read.option("header", "false")
        .option("sep", "\t")
        .csv(cities_path)
    )

    ok_rs_cities_id = source_ok_rs_cities_id.select(
        sf.col("_c0").cast(IntegerType()).alias("city_ok_id"),
        sf.col("_c1").cast(IntegerType()).alias("city_rs_id"),
    )

    return ok_rs_cities_id


def get_rs_cities(spark, name_cities_path):
    """
    Имена городов с ID Росстата
    """

    source_rs_cities_id = (
        spark.read.option("header", "false")
        .option("sep", ";")
        .csv(name_cities_path)
    )

    rs_cities_df = source_rs_cities_id.select(
        sf.col("_c0").cast(StringType()).alias("city_name"),
        sf.col("_c1").cast(IntegerType()).alias("city_id"),
    )

    return rs_cities_df


def get_ok_df(spark, russia_id, demography_path, current_dt):
    """
    Таблица ОК жителей России с полом, возрастом () и городом
    """
    source_ok_df = (
        spark.read.option("header", "false")
        .option("sep", "\t")
        .csv(demography_path)
    )

    source_ok_df = source_ok_df.where(
        sf.col("_c4") == russia_id.first()["country_id"]
        )

    ok_df = source_ok_df.select(
        sf.floor(
            sf.datediff(
                sf.to_date(sf.lit(current_dt)),
                sf.from_unixtime(sf.col("_c2").cast(IntegerType()) * (24 * 60 * 60)),
            ) / 365.25
        ).alias("age"),
        sf.col("_c3").cast(IntegerType()).alias("sex"),
        sf.col("_c5").cast(IntegerType()).alias("city_ok_id"),
    )
    return ok_df


def get_ok_dem_df(ok_df, names_ok_cities_df):
    """
    Финальная таблица
    """
    men_id = 1
    women_id = 2

    ok_dem_df = (
        ok_df.groupBy("city_ok_id")
        .agg(
            sf.count("*").alias("user_cnt"),
            sf.round(sf.avg("age"), 2).alias("age_avg"),
            sf.count(sf.when(sf.col("sex") == men_id, 1)).alias("men_cnt"),
            sf.count(sf.when(sf.col("sex") == women_id, 1)).alias("women_cnt"),
        )
        .withColumn("men_share", sf.round(sf.col("men_cnt") / sf.col("user_cnt"), 2))
        .withColumn(
            "women_share", sf.round(sf.col("women_cnt") / sf.col("user_cnt"), 2)
        )
        .join(names_ok_cities_df, on="city_ok_id", how="inner")
        .select(
            sf.col("city_name"),
            sf.col("user_cnt"),
            sf.col("age_avg"),
            sf.col("men_cnt"),
            sf.col("women_cnt"),
            sf.col("men_share"),
            sf.col("women_share"),
        )
    )
    return ok_dem_df


def main(argv):
    price_stat_path = argv[1]
    prices_path = argv[2]
    country_path = argv[3]
    name_cities_path = argv[4]
    cities_path = argv[5]
    demography_path = argv[6]
    current_dt = argv[7]
    ok_dem_result_path = argv[8]

    spark = (
        pyspark.sql.SparkSession.builder.getOrCreate()
    )

    cities_df = get_cities_df(spark, price_stat_path, prices_path)
    russia_id = get_russia_id(spark, country_path)
    ok_rs_cities_id = get_ok_rs_cities_id(spark, cities_path)
    rs_cities_df = get_rs_cities(spark, name_cities_path)

    # Имена городов c id ok, которые есть в cities_df
    names_ok_cities_df = (
        rs_cities_df.join(
            cities_df, rs_cities_df.city_id == cities_df.city_id, how="left"
        )
        .join(
            ok_rs_cities_id,
            rs_cities_df.city_id == ok_rs_cities_id.city_rs_id,
            how="left",
        )
        .select(sf.col("city_name"), sf.col("city_ok_id"))
    )

    ok_df = get_ok_df(spark, russia_id, demography_path, current_dt)
    result = get_ok_dem_df(ok_df, names_ok_cities_df)

    (
        result.repartition(1)
        .sortWithinPartitions(sf.col("user_cnt").desc())
        .write.mode("overwrite")
        .option("header", "true")
        .option("sep", ";")
        .csv(ok_dem_result_path)
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv))
