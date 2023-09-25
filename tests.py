import pytest
from pyspark.sql import SparkSession
from script import *

@pytest.fixture(scope="module")
def spark_session():
    return SparkSession.builder.appName("Test").getOrCreate()

def test_read_data(spark_session):
    input = "data/hotels.csv"  
    expected_row_count = 119390  
    result = read_data(spark_session, input)

    assert result.count() == expected_row_count

def test_filter_data(spark_session):
    data = [(1, "Offline TA/TO"), (2, "Online TA"), (3, "Offline TA/TO")]
    schema = ["booking_id", "market_segment"]
    expected_row_count = 2  
    input = spark_session.createDataFrame(data, schema=schema)
    result = filter_data(input)

    assert result.count() == expected_row_count

def test_add_arrival_date(spark_session):
    data = [(2023, 9, 25, 2, 2), (2023, 9, 26, 1, 3)]
    schema = ["arrival_date_year", "arrival_date_month", "arrival_date_day_of_month", 
              "stays_in_weekend_nights", "stays_in_week_nights"]
    expected_row_count = 2 
    input = spark_session.createDataFrame(data, schema=schema)

    result = add_arrival_date(input)

    assert result.count() == expected_row_count

def test_calculate_departure_date(spark_session):
    data = [(2023, 9, 25, 2, 2), (2023, 9, 26, 1, 3)]
    schema = ["arrival_date_year", "arrival_date_month", "arrival_date_day_of_month",
              "stays_in_weekend_nights", "stays_in_week_nights"]
    expected_row_count = 2  

    input = spark_session.createDataFrame(data, schema=schema)
    
   
    input_with_arrival_date = add_arrival_date(input)
    
    result = calculate_departure_date(input_with_arrival_date)

    assert result.count() == expected_row_count

def test_calculate_with_family_breakfast(spark_session):
    data = [(1, 1, 0), (2, 0, 2)]
    schema = ["children", "babies"]
    expected_row_count = 2  

    input = spark_session.createDataFrame(data, schema=schema)
    result = calculate_with_family_breakfast(input)

    assert result.count() == expected_row_count
