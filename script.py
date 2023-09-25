from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, date_add, col, when

def main(input, output_path):
    spark = SparkSession.builder.appName("BookingETL").getOrCreate()

    try:
        df = read_data(spark, input)

        transformed_df = (
            df
            .transform(filter_data)
            .transform(add_arrival_date)
            .transform(calculate_departure_date)
            .transform(calculate_with_family_breakfast)
        )
        transformed_df.show(5)
        transformed_df.write.mode("overwrite").parquet(output_path)
    finally:
        spark.stop()

def read_data(spark, input):
    return spark.read.option("header", "true").csv(input)

def filter_data(df):
    return df.filter(col("market_segment") == "Offline TA/TO")

def add_arrival_date(df):
    return df.withColumn(
        "arrival_date",
        concat_ws("-",
                   col("arrival_date_year"),
                   col("arrival_date_month"),
                   col("arrival_date_day_of_month"))
    )

def calculate_departure_date(df):
    return df.withColumn(
        "departure_date",
        date_add(
            col("arrival_date"),
            (col("stays_in_weekend_nights").cast("int") + col("stays_in_week_nights").cast("int"))
        )
    )

def calculate_with_family_breakfast(df):
    return df.withColumn(
        "with_family_breakfast",
        when(col("children") + col("babies") > 0, "Yes").otherwise("No")
    )

if __name__ == "__main__":
    input = "data/hotels.csv"
    output_path = "output/output.parquet"
    main(input, output_path)
