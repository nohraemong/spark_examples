# GDELT Event Data Processor

## 프로젝트 개요
GDELT(Global Database of Events, Language, and Tone) 이벤트 데이터를 처리하여 이벤트 분석 및 요약 정보를 생성하는 Spark 애플리케이션

## 주요 기능
1. **데이터 중복 제거**
   - GlobalEventID 기준 중복 확인 및 제거
   - 최신 데이터 기준(DATEADDED) 유지

2. **이벤트 요약 (Summary) 생성**
   - 2017-2019년 데이터만 포함
   - 부정적인 이벤트(AvgTone < 0)만 선택
   - 국가별, 연도별로 가장 적게 발생한 3개 CAMEO 이벤트 타입 추출
   - 연도별 상위 15개 이벤트 선택
   - TSV 형식의 단일 파일로 저장

3. **전체 데이터 (Full Output) 저장**
   - Snappy 압축 Parquet 파일 형식
   - Year, Country Code, CAMEO Verb 기준 파티셔닝
   - 파티션당 단일 파일 구조
   - Hive 동적 파티셔닝

## 출력 구조

### 1. Summary 출력
s3://output-path/gdelt_summary/part-00000-xxx.csv

포맷:
```
Year    CountryCode    CAMEO_Verb    num_events
2017    US            PROTEST        127612
2017    US            EXHIBIT FORCE  30865
2017    IN            REDUCE        17689
```

### 2. Full Output

```
gdelt_full_output/
├── year=2017/
│   ├── countrycode=AA/
│   │   ├── cameo_verb=APPEAL/
│   │   │   └── part-00000-xxx.snappy.parquet
│   │   ├── cameo_verb=ASSAULT/
│   │   │   └── part-00000-xxx.snappy.parquet
```


참고 문서

    [GDELT 2.0 API 문서](http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf)
    [CAMEO Codebook](http://data.gdeltproject.org/documentation/CAMEO.Manual.1.1b3.pdf)
