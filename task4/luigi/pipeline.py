import os
import luigi
import luigi.contrib.spark
import luigi.contrib.hdfs

USER = os.environ.get("USER")


class PriceStat(luigi.contrib.spark.SparkSubmitTask):

    app = "price_stat.py"
    master = "local[*]"

    output_path = luigi.Parameter()
    prices_path = luigi.Parameter()
    products_for_stat_path = luigi.Parameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.output_path.format(USER))

    def requires(self):
        pass

    def app_options(self):
        return [
            self.prices_path.format(USER),
            self.products_for_stat_path.format(USER),
            self.output_path.format(USER),
        ]


class OkDem(luigi.contrib.spark.SparkSubmitTask):

    app = "ok_dem.py"
    master = "local[*]"

    output_path = luigi.Parameter()
    prices_path = luigi.Parameter()
    price_stat_path = luigi.Parameter()
    name_cities_path = luigi.Parameter()
    cities_path = luigi.Parameter()
    demography_path = luigi.Parameter()
    country_path = luigi.Parameter()
    current_dt = luigi.Parameter(default="2023-03-01")

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.output_path.format(USER))

    def requires(self):
        return PriceStat()

    def app_options(self):
        return [
            self.price_stat_path.format(USER),
            self.prices_path.format(USER),
            self.country_path.format(USER),
            self.name_cities_path.format(USER),
            self.cities_path.format(USER),
            self.demography_path.format(USER),
            self.current_dt.format(USER),
            self.output_path.format(USER),
        ]


class ProductStat(luigi.contrib.spark.SparkSubmitTask):

    app = "product_stat.py"
    master = "local[*]"

    output_path = luigi.Parameter()
    prices_path = luigi.Parameter()
    ok_dem_path = luigi.Parameter()
    name_cities_path = luigi.Parameter()
    products_path = luigi.Parameter()

    def output(self):
        return luigi.contrib.hdfs.HdfsTarget(self.output_path.format(USER))

    def requires(self):
        return OkDem()

    def app_options(self):
        return [
            self.prices_path.format(USER),
            self.ok_dem_path.format(USER),
            self.name_cities_path.format(USER),
            self.products_path.format(USER),
            self.output_path.format(USER),
        ]


if __name__ == "__main__":
    luigi.run()
