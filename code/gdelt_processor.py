from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
import sys

def create_spark_session():
    return SparkSession.builder \
        .appName("GDELT Event Processor") \
        .getOrCreate()


# 스키마 제공
gdelt_schema = StructType([
    # EVENTID AND DATE ATTRIBUTES
    StructField("GlobalEventID", LongType(), True),
    StructField("Day", IntegerType(), True),
    StructField("MonthYear", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("FractionDate", FloatType(), True),
    
    # ACTOR 1 ATTRIBUTES
    StructField("Actor1Code", StringType(), True),
    StructField("Actor1Name", StringType(), True),
    StructField("Actor1CountryCode", StringType(), True),
    StructField("Actor1KnownGroupCode", StringType(), True),
    StructField("Actor1EthnicCode", StringType(), True),
    StructField("Actor1Religion1Code", StringType(), True),
    StructField("Actor1Religion2Code", StringType(), True),
    StructField("Actor1Type1Code", StringType(), True),
    StructField("Actor1Type2Code", StringType(), True),
    StructField("Actor1Type3Code", StringType(), True),
    
    # ACTOR 2 ATTRIBUTES
    StructField("Actor2Code", StringType(), True),
    StructField("Actor2Name", StringType(), True),
    StructField("Actor2CountryCode", StringType(), True),
    StructField("Actor2KnownGroupCode", StringType(), True),
    StructField("Actor2EthnicCode", StringType(), True),
    StructField("Actor2Religion1Code", StringType(), True),
    StructField("Actor2Religion2Code", StringType(), True),
    StructField("Actor2Type1Code", StringType(), True),
    StructField("Actor2Type2Code", StringType(), True),
    StructField("Actor2Type3Code", StringType(), True),
    
    # EVENT ACTION ATTRIBUTES
    StructField("IsRootEvent", IntegerType(), True),
    StructField("EventCode", StringType(), True),
    StructField("EventBaseCode", StringType(), True),
    StructField("EventRootCode", StringType(), True),
    StructField("QuadClass", IntegerType(), True),
    StructField("GoldsteinScale", FloatType(), True),
    StructField("NumMentions", IntegerType(), True),
    StructField("NumSources", IntegerType(), True),
    StructField("NumArticles", IntegerType(), True),
    StructField("AvgTone", FloatType(), True),
    
    # ACTOR 1 GEO ATTRIBUTES
    StructField("Actor1Geo_Type", IntegerType(), True),
    StructField("Actor1Geo_FullName", StringType(), True),
    StructField("Actor1Geo_CountryCode", StringType(), True),
    StructField("Actor1Geo_ADM1Code", StringType(), True),
    StructField("Actor1Geo_ADM2Code", StringType(), True),
    StructField("Actor1Geo_Lat", FloatType(), True),
    StructField("Actor1Geo_Long", FloatType(), True),
    StructField("Actor1Geo_FeatureID", StringType(), True),
    
    # ACTOR 2 GEO ATTRIBUTES
    StructField("Actor2Geo_Type", IntegerType(), True),
    StructField("Actor2Geo_FullName", StringType(), True),
    StructField("Actor2Geo_CountryCode", StringType(), True),
    StructField("Actor2Geo_ADM1Code", StringType(), True),
    StructField("Actor2Geo_ADM2Code", StringType(), True),
    StructField("Actor2Geo_Lat", FloatType(), True),
    StructField("Actor2Geo_Long", FloatType(), True),
    StructField("Actor2Geo_FeatureID", StringType(), True),
    
    # ACTION GEO ATTRIBUTES
    StructField("ActionGeo_Type", IntegerType(), True),
    StructField("ActionGeo_FullName", StringType(), True),
    StructField("ActionGeo_CountryCode", StringType(), True),
    StructField("ActionGeo_ADM1Code", StringType(), True),
    StructField("ActionGeo_ADM2Code", StringType(), True),
    StructField("ActionGeo_Lat", FloatType(), True),
    StructField("ActionGeo_Long", FloatType(), True),
    StructField("ActionGeo_FeatureID", StringType(), True),
    
    # DATA MANAGEMENT FIELDS
    StructField("DATEADDED", LongType(), True),
    StructField("SOURCEURL", StringType(), True)
])


def check_and_remove_duplicates(gdelt_df):
    """
    GDELT 이벤트 데이터 중복 확인 및 제거
  
    - GlobalEventID: 각 이벤트 레코드의 고유 식별자
    - DATEADDED: 이벤트가 데이터베이스에 추가된 시간 (YYYYMMDDHHMMSS)
    - (Day, Actor1Code, Actor2Code, EventCode): 이벤트의 핵심 속성
    """
    
    # 1. GlobalEventID 기준 중복 확인
    eventid_duplicates = gdelt_df.groupBy("GlobalEventID") \
        .count() \
        .filter(col("count") > 1)
    
    print(f"GlobalEventID duplicates: {eventid_duplicates.count()}")
    
    # 2. 핵심 필드 조합으로 중복 확인
    core_fields = ["Day", "Actor1Code", "Actor2Code", "EventCode", "DATEADDED"]
    
    core_duplicates = gdelt_df.groupBy(core_fields) \
        .count() \
        .filter(col("count") > 1)
    
    print(f"duplicates by fields: {core_duplicates.count()}")
    
    # 3. 시간대별 중복 패턴 분석
    time_window = Window.partitionBy("Day", "Actor1Code", "Actor2Code", "EventCode") \
        .orderBy("DATEADDED")
    
    duplicate_patterns = gdelt_df \
        .withColumn("duplicate_rank", row_number().over(time_window)) \
        .filter(col("duplicate_rank") > 1)
    
    print(f"duplicates by time window: {duplicate_patterns.count()}")
    
    # 4. 중복 상세 분석
    print("\nduplicate records:")
    duplicate_patterns \
        .groupBy("Year", "MonthYear") \
        .count() \
        .orderBy("Year", "MonthYear") \
        .show()
    
    # 5. 중복 제거
    # DATEADDED를 기준으로 가장 최신 레코드만 유지
    deduplicated_df = gdelt_df \
        .withColumn("row_num", row_number().over(
            Window.partitionBy("GlobalEventID")
            .orderBy(desc("DATEADDED"))
        )) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # 결과 통계
    print("\ndeduplicates result:")
    print(f"original record counts: {gdelt_df.count()}")
    print(f"after deduplicates: {deduplicated_df.count()}")
    print(f"deduplicates count: {gdelt_df.count() - deduplicated_df.count()}")
    
    return deduplicated_df

def map_to_cameo_verb(event_code):
    if event_code is None or len(event_code) < 2:
        return "OTHER"
    
    # 처음 두 자리를 정수로 변환
    try:
        base_code = int(event_code[:2])
        base_code_str = f"{base_code:02d}"
    except ValueError:
        return "OTHER"
    
    print(f"Input EventCode: '{event_code}', Base code: '{base_code_str}'") 
    
    cameo_dict = {
        "01": "MAKE STATEMENT",
        "02": "APPEAL",
        "03": "EXPRESS INTENT",
        "04": "CONSULT",
        "05": "ENGAGE",
        "06": "COOPERATE",
        "07": "PROVIDE AID",
        "08": "YIELD",
        "09": "INVESTIGATE",
        "10": "DEMAND",
        "11": "DISAPPROVE",
        "12": "REJECT",
        "13": "THREATEN",
        "14": "PROTEST",
        "15": "EXHIBIT FORCE",
        "16": "REDUCE RELATIONS",
        "17": "COERCE",
        "18": "ASSAULT",
        "19": "FIGHT",
        "20": "USE UNCONVENTIONAL MASS VIOLENCE"
    }
    
    result = cameo_dict.get(base_code_str, "OTHER")
    print(f"Mapping result: {result}") 
    return result

# UDF 등록, summary 함수 내에서 이용
map_cameo_udf = udf(map_to_cameo_verb, StringType())

# Summary 함수
def process_gdelt_events(df, output_path):
    # 기본 데이터 필터링 및 CAMEO verb dataframe에 추가
    base_df = df \
        .filter(col("Year").isin(2017, 2018, 2019)) \
        .filter(col("AvgTone") < 0) \
        .filter(col("Actor1CountryCode").isNotNull()) \
        .withColumn("cameo_verb", map_cameo_udf(col("EventCode")))

    # 이벤트 수 집계
    event_counts = base_df \
        .groupBy("Year", "Actor1CountryCode", "cameo_verb") \
        .agg(count("*").alias("num_events"))

    # 국가별, 연도별로 가장 적은 이벤트 3개의 CAMEO verb 찾기
    country_year_window = Window \
        .partitionBy("Year", "Actor1CountryCode") \
        .orderBy("num_events")

    least_events = event_counts \
        .withColumn("country_rank", row_number().over(country_year_window)) \
        .filter(col("country_rank") <= 3)

    # 연도별 상위 15개 이벤트 선택
    year_window = Window \
        .partitionBy("Year") \
        .orderBy(desc("num_events"))

    final_results = least_events \
        .withColumn("year_rank", row_number().over(year_window)) \
        .filter(col("year_rank") <= 15) \
        .select(
            "Year",
            "Actor1CountryCode",
            "cameo_verb",
            "num_events"
        ) \
        .orderBy("Year", desc("num_events"))
    
    final_results.show()
    final_results.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "false") \
        .option("delimiter", "\t") \
        .csv(output_path)
    


def save_full_output(deduplicated_df, output_path):
    """
    중복 제거된 데이터를 요구사항에 맞게 파티셔닝하여 저장
    
    Requirements:
    1. Snappy compressed parquet files
    2. 2017-2019년 데이터만 포함
    3. Year-month and tone
    4. Year, countrycode, cameo_verb로 파티셔닝
    5. 파티션당 단일 파일 저장되도록
    6. Hive 동적 파티셔닝 구조
    
    ├── year=2017
│   ├── countrycode=AA
│   │   ├── cameo_verb=APPEAL
│   │   │   └── part-00000-de038c54-f84a-47bc-be0a-5d21618e6439.c000.snappy.parquet
│   │   ├── cameo_verb=ASSAULT
│   │   │   └── part-00000-de038c54-f84a-47bc-be0a-5d21618e6439.c000.snappy.parquet
...
│   ├── countrycode=AC
│   │   ├── cameo_verb=APPEAL
│   │   │   └── part-00000-de038c54-f84a-47bc-be0a-5d21618e6439.c000.snappy.parquet
│   │   ├── cameo_verb=ASSAULT
│   │   │   └── part-00000-de038c54-f84a-47bc-be0a-5d21618e6439.c000.snappy.parquet
...
├── year=2018
│   ├── countrycode=HO
│   │   ├── cameo_verb=APPEAL
│   │   │   └── part-00397-de038c54-f84a-47bc-be0a-5d21618e6439.c000.snappy.parquet
...
    """
    try:
        # 1. 기본 필터링 및 데이터 변환
        processed_df = deduplicated_df \
            .filter(col("Year").isin(2017, 2018, 2019)) \
            .withColumn("cameo_verb", map_cameo_udf(col("EventCode"))) \
            .withColumn("yearmonth", col("MonthYear").cast("string")) \
            .select(
                # Non-partition columns
                col("yearmonth"),
                col("AvgTone").alias("tone"),
                # Partition columns
                col("Year").alias("year"),
                col("Actor1CountryCode").alias("countrycode"),
                col("cameo_verb")
            )

        # 2. 파티션별로 단일 파일 저장
        processed_df \
            .repartition("year", "countrycode", "cameo_verb") \
            .write \
            .partitionBy("year", "countrycode", "cameo_verb") \
            .option("compression", "snappy") \
            .option("maxRecordsPerFile", 1000000) \
            .mode("overwrite") \
            .format("parquet") \
            .save(output_path)

        print("Full output saved successfully")
        
        # 3. 저장된 데이터 검증
        saved_df = spark.read.parquet(output_path)
        
        print("\nValidation Results:")
        print(f"Total records: {saved_df.count()}")
        print("\nPartition distribution:")
        saved_df.groupBy("year", "countrycode", "cameo_verb") \
            .count() \
            .orderBy("year", "countrycode", "cameo_verb") \
            .show(5)

        return True

    except Exception as e:
        print(f"Error saving full output: {str(e)}")
        return False

def main():
    if len(sys.argv) != 4:
        print("Usage: spark-submit gdelt_processor.py <input_path> <summary_output_path> <full_output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    summary_output_path = sys.argv[2]
    full_output_path = sys.argv[3]

    # SparkSession 생성
    spark = create_spark_session()
    
    try:
        # 데이터 읽기
        gdelt_df = spark.read \
            .option("delimiter", "\t") \
            .option("header", False) \
            .schema(gdelt_schema) \
            .csv(input_path)
        
        # 1. 중복 제거
        print("Starting deduplication process...")
        deduplicated_df = check_and_remove_duplicates(gdelt_df)
        
        # 2. Summary 파일 생성
        print("\nCreating summary file...")
        process_gdelt_events(deduplicated_df, summary_output_path) 
        
        # 3. Full output 저장
        print("\nSaving full output...")
        success = save_full_output(deduplicated_df, full_output_path)
        
        if success:
            print("\nAll processes completed successfully")
        else:
            print("\nError occurred during processing")
            sys.exit(1)
            
    finally:
        # SparkSession 종료
        spark.stop()

if __name__ == "__main__":
    main()
