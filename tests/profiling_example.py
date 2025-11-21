def run_data_profiling(config_json: str):
    """
    Main profiling function called from Composer
    """
    import json
    from datetime import datetime
    
    config = json.loads(config_json)
    spark = SparkSession.builder.appName("DataProfiler").getOrCreate()
    
    profiler = SimpleDataProfiler(spark)
    
    # Profile tables
    profile_results = profiler.profile_multiple_tables(
        table_list=config['tables'],
        fecha=config.get('fecha')  # Optional date filter
    )
    
    # Add run metadata
    profile_results = profile_results.withColumn("run_id", lit(config.get('run_id', 'unknown'))) \
                                   .withColumn("environment", lit(config.get('environment', 'prod')))
    
    # Save to BigQuery
    profile_results.write \
        .format("bigquery") \
        .option("table", config['output_table']) \
        .option("writeMethod", "APPEND") \
        .save()
    
    # Show summary
    profile_results.show(truncate=False)
    
    spark.stop()

# matching config:

# # Data profiling configuration
# environment: "prod"
# output_table: "your-project.monitoring.data_profiles"

# # Tables to profile
# tables:
#   - "your-project.raw.transactions"
#   - "your-project.raw.accounts" 
#   - "your-project.raw.customers"
#   - "your-project.curated.customer_360"
#   - "your-project.curated.risk_metrics"

# # Optional: filter by specific date
# # fecha: "2024-11-20"  # If not provided, profiles entire table

# # Profiling settings
# settings:
#   sample_size: null  # null = full table, or specify number like 100000
#   include_schema_info: true
#   calculate_histograms: false  # Keep simple for now


# orchestration example:

# python# Add to your existing DAG factory
# profiling_dag = create_generic_dag(
#     dag_id='data_profiling_daily',
#     script_name='data_profiler.py',
#     schedule_interval='0 7 * * *',  # 7 AM daily, before DQ checks
#     description='Daily data profiling for key tables'
# )
