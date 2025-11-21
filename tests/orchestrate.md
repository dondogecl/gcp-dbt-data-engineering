Running the sample dq tests:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from datetime import datetime

class SimpleDataQualityFunctions:
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def completeness_check(self, table_name: str, fecha: str, columns: list):
        """
        Check for missing/null values in specified columns
        Args:
            table_name: BigQuery table reference
            fecha: Date to check (YYYY-MM-DD)
            columns: List of column names to check
        Returns:
            DataFrame with test results
        """
        df = self.spark.read.format("bigquery").load(table_name)
        df_filtered = df.filter(col("fecha") == fecha)
        
        total_count = df_filtered.count()
        results = []
        
        for column in columns:
            null_count = df_filtered.filter(
                col(column).isNull() | 
                (col(column) == "") | 
                isnan(col(column))
            ).count()
            
            results.append({
                'test_date': fecha,
                'test_name': f'completeness_{column}',
                'test_category': 'completeness',
                'status': 'PASS' if null_count == 0 else 'FAIL',
                'failed_records': null_count,
                'total_records': total_count,
                'failure_rate': null_count / total_count if total_count > 0 else 0,
                'column_name': column
            })
        
        return self.spark.createDataFrame(results)
    
    def uniqueness_check(self, table_name: str, fecha: str, key_combinations: list):
        """
        Check for duplicate records based on key combinations
        Args:
            table_name: BigQuery table reference
            fecha: Date to check
            key_combinations: List of column lists (e.g., [['ruc'], ['ruc', 'code_operation']])
        """
        df = self.spark.read.format("bigquery").load(table_name)
        df_filtered = df.filter(col("fecha") == fecha)
        
        total_count = df_filtered.count()
        results = []
        
        for keys in key_combinations:
            # Count duplicates
            duplicate_df = df_filtered.groupBy(*keys).count().filter(col("count") > 1)
            duplicate_count = duplicate_df.count()
            
            key_name = "_".join(keys)
            results.append({
                'test_date': fecha,
                'test_name': f'uniqueness_{key_name}',
                'test_category': 'uniqueness',
                'status': 'PASS' if duplicate_count == 0 else 'FAIL',
                'failed_records': duplicate_count,
                'total_records': total_count,
                'failure_rate': duplicate_count / total_count if total_count > 0 else 0,
                'key_columns': ",".join(keys)
            })
        
        return self.spark.createDataFrame(results)
    
    def format_validation(self, table_name: str, fecha: str, format_rules: dict):
        """
        Validate column formats
        Args:
            table_name: BigQuery table reference
            fecha: Date to check
            format_rules: Dict like {'currency': 'length_3', 'customer_rate': 'positive'}
        """
        df = self.spark.read.format("bigquery").load(table_name)
        df_filtered = df.filter(col("fecha") == fecha)
        
        total_count = df_filtered.count()
        results = []
        
        for column, rule in format_rules.items():
            failed_count = 0
            
            if rule == 'length_3':
                failed_count = df_filtered.filter(
                    ~col(column).rlike("^[A-Z]{3}$")
                ).count()
            elif rule == 'positive':
                failed_count = df_filtered.filter(
                    col(column) <= 0
                ).count()
            elif rule == 'not_empty':
                failed_count = df_filtered.filter(
                    col(column).isNull() | (col(column) == "")
                ).count()
            
            results.append({
                'test_date': fecha,
                'test_name': f'format_{column}_{rule}',
                'test_category': 'format',
                'status': 'PASS' if failed_count == 0 else 'FAIL',
                'failed_records': failed_count,
                'total_records': total_count,
                'failure_rate': failed_count / total_count if total_count > 0 else 0,
                'column_name': column,
                'rule': rule
            })
        
        return self.spark.createDataFrame(results)
    
    def range_validation(self, table_name: str, fecha: str, range_rules: dict):
        """
        Check if values are within expected ranges
        Args:
            table_name: BigQuery table reference
            fecha: Date to check
            range_rules: Dict like {'customer_rate': {'min': 0, 'max': 100}}
        """
        df = self.spark.read.format("bigquery").load(table_name)
        df_filtered = df.filter(col("fecha") == fecha)
        
        total_count = df_filtered.count()
        results = []
        
        for column, bounds in range_rules.items():
            min_val = bounds.get('min')
            max_val = bounds.get('max')
            
            condition = col(column).isNotNull()
            if min_val is not None:
                condition = condition & (col(column) >= min_val)
            if max_val is not None:
                condition = condition & (col(column) <= max_val)
            
            failed_count = df_filtered.filter(~condition).count()
            
            results.append({
                'test_date': fecha,
                'test_name': f'range_{column}',
                'test_category': 'range',
                'status': 'PASS' if failed_count == 0 else 'FAIL',
                'failed_records': failed_count,
                'total_records': total_count,
                'failure_rate': failed_count / total_count if total_count > 0 else 0,
                'column_name': column,
                'min_value': min_val,
                'max_value': max_val
            })
        
        return self.spark.createDataFrame(results)
    
    def custom_sql_check(self, table_name: str, fecha: str, custom_tests: list):
        """
        Execute custom SQL validation rules
        Args:
            table_name: BigQuery table reference
            fecha: Date to check
            custom_tests: List of dicts with test_name and sql_condition
        """
        df = self.spark.read.format("bigquery").load(table_name)
        df_filtered = df.filter(col("fecha") == fecha)
        df_filtered.createOrReplaceTempView("temp_table")
        
        total_count = df_filtered.count()
        results = []
        
        for test in custom_tests:
            test_name = test['test_name']
            sql_condition = test['sql_condition']
            
            # Execute custom SQL
            failed_df = self.spark.sql(f"""
                SELECT COUNT(*) as failed_count 
                FROM temp_table 
                WHERE NOT ({sql_condition})
            """)
            
            failed_count = failed_df.collect()[0]['failed_count']
            
            results.append({
                'test_date': fecha,
                'test_name': f'custom_{test_name}',
                'test_category': 'custom',
                'status': 'PASS' if failed_count == 0 else 'FAIL',
                'failed_records': failed_count,
                'total_records': total_count,
                'failure_rate': failed_count / total_count if total_count > 0 else 0,
                'sql_condition': sql_condition
            })
        
        return self.spark.createDataFrame(results)
```

Then calling it from an orchestrator tool (dag):

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSFileTransformOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
import yaml
import json

def load_job_config(**context):
    """Load job-specific configuration from YAML"""
    dag_id = context['dag'].dag_id
    
    # Configuration files stored in GCS
    config_file = f"gs://your-config-bucket/dq-jobs/{dag_id}.yaml"
    
    from google.cloud import storage
    client = storage.Client()
    bucket_name = config_file.replace('gs://', '').split('/')[0]
    blob_path = '/'.join(config_file.replace('gs://', '').split('/')[1:])
    
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    
    config_yaml = blob.download_as_text()
    config = yaml.safe_load(config_yaml)
    
    # Convert YAML to JSON for Dataproc job
    config_json = json.dumps(config)
    
    return config_json

# Generic DAG configuration
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['your-team@company.com']
}

def create_dq_dag(dag_id: str, schedule_interval: str, description: str):
    """Factory function to create DQ DAGs"""
    
    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule_interval=schedule_interval,
        catchup=False,
        max_active_runs=1,
        tags=['data-quality', 'banking']
    )
    
    with dag:
        # Task 1: Load job configuration
        load_config_task = PythonOperator(
            task_id='load_job_config',
            python_callable=load_job_config,
            provide_context=True
        )
        
        # Task 2: Run data quality checks
        run_dq_checks = DataprocSubmitJobOperator(
            task_id='run_data_quality_checks',
            project_id=Variable.get('project_id'),
            region=Variable.get('region'),
            job={
                "reference": {"job_id": f"dq-{dag_id}-{{{{ ds_nodash }}}}"},
                "placement": {"cluster_name": ""},  # Serverless
                "pyspark_job": {
                    "main_python_file_uri": Variable.get('dq_script_uri'),  # gs://bucket/scripts/dq_checker.py
                    "args": [
                        "--config", "{{ ti.xcom_pull(task_ids='load_job_config') }}",
                        "--fecha", "{{ ds }}",
                        "--execution_date", "{{ ts }}"
                    ],
                    "properties": {
                        "spark.executor.instances": "2",
                        "spark.executor.memory": "4g", 
                        "spark.driver.memory": "2g",
                        "spark.sql.execution.timeout": "300s"
                    }
                }
            },
            asynchronous=False
        )
        
        # Task dependencies
        load_config_task >> run_dq_checks
    
    return dag

# Create specific DAG instances
transactions_dq_dag = create_dq_dag(
    dag_id='dq_transactions_daily',
    schedule_interval='0 9 * * *',  # 9 AM daily
    description='Daily data quality checks for transactions table'
)

accounts_dq_dag = create_dq_dag(
    dag_id='dq_accounts_daily', 
    schedule_interval='0 10 * * *',  # 10 AM daily
    description='Daily data quality checks for accounts table'
)

customer_dq_dag = create_dq_dag(
    dag_id='dq_customers_weekly',
    schedule_interval='0 8 * * 1',  # Monday 8 AM
    description='Weekly data quality checks for customer data'
)
```
