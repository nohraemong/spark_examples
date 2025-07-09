spark-submit --deploy-mode cluster \
    --driver-memory 6g \
    --executor-memory 10g \
    --executor-cores 6 \
    --num-executors 9 \
    --conf spark.sql.shuffle.partitions=144 \
    gdelt_processor.py {input_data_uri} {summary_data_uri} {output_data_uri}
    
