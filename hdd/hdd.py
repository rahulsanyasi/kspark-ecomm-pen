from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def start_spark_session(local_app_name: str) -> SparkSession:
    print(f"start_spark_session|")

    spark = SparkSession.bulder.master("local[1]").appName(local_app_name).getOrCreate()

    return spark


def main() -> None:
    print(f"main|")


if __name__ == "__main__":
    print(f"start-up-block")
    main()
