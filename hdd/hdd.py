from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def read_csv_file(spark:SparkSession, file_name:str) -> DataFrame:
    print(f"read_csv_file | spark:{spark} | file_name:{file_name}")
    my_df: DataFrame = spark.read.options(header='True', inferSchema= 'True').csv(file_name)
    entry_count: int = my_df.count()
    print(f"read_csv_file | entry_count: {entry_count}")
    my_df.show()
    return my_df

def start_spark_session(local_app_name: str) -> SparkSession:
    print(f"start_spark_session|")

    spark = (SparkSession.builder\
        .master("local[1]").appName(local_app_name)\
        .getOrCreate())

    return spark

def main() -> None:
    print(f"main|")

app_name: str = "rahul-app"
my_ss: SparkSession = start_spark_session(app_name)
new_df: DataFrame = read_csv_file(my_ss, "hdd.csv")
print(f"main |new_df:{new_df}")

if __name__ == "__main__":
    print(f"start-up-block")
    main()