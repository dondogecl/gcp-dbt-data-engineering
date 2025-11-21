## Idea for a yaml configuration template:

```yaml
# DAG configuration
data_quality_config = {
    "table_name": "your_project.dataset.table_name",
    "fecha": "{{ ds }}",
    "tests": {
        "completeness": ["source", "rut", "nombre_cliente"],
        "uniqueness": [["rut", "code_operation"]],
        
        # Complex rules with inline SQL
        "custom_sql": [
            {
                "test_name": "valid_account_hierarchy",
                "description": "Check account hierarchy consistency",
                "sql": """
                    SELECT COUNT(*) as failed_records
                    FROM {table_name} 
                    WHERE fecha = '{fecha}'
                      AND cuentabt IS NOT NULL 
                      AND cuenta_integradora IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1 FROM `project.reference.account_hierarchy` h
                          WHERE h.sub_account = cuentabt 
                            AND h.parent_account = cuenta_integradora
                      )
                """,
                "expected_result": 0,
                "severity": "ERROR"
            },
            {
                "test_name": "consistent_currency_rates", 
                "description": "Customer rate should align with currency standards",
                "sql": """
                    SELECT COUNT(*) as failed_records
                    FROM {table_name} t
                    LEFT JOIN `project.reference.currency_rates` r 
                      ON t.currency = r.currency_code 
                      AND t.fecha = r.rate_date
                    WHERE t.fecha = '{fecha}'
                      AND ABS(t.customer_rate - r.standard_rate) > r.tolerance_threshold
                """,
                "expected_result": 0,
                "severity": "WARNING"
            }
        ]
    }
}
```

## typical dq queries

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


## SQL injection considerations:

```py
import re
from datetime import datetime
from typing import Dict, List, Any

class DataQualitySecurityValidator:
    """Validates and sanitizes DQ configuration inputs"""
    
    # Allowed characters for identifiers
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*$')
    
    # Forbidden SQL keywords (case-insensitive)
    FORBIDDEN_KEYWORDS = {
        'drop', 'truncate', 'delete', 'insert', 'update', 'create', 
        'alter', 'exec', 'execute', 'sp_', 'xp_', 'grant', 'revoke',
        'merge', 'bulk', 'openquery', 'openrowset', 'dbcc'
    }
    
    # Allowed table name pattern (project.dataset.table)
    TABLE_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$')
    
    @classmethod
    def validate_table_name(cls, table_name: str) -> bool:
        """Validate BigQuery table name format"""
        if not cls.TABLE_PATTERN.match(table_name):
            raise ValueError(f"Invalid table name format: {table_name}")
        return True
    
    @classmethod
    def validate_column_names(cls, columns: List[str]) -> bool:
        """Validate column names contain only safe characters"""
        for col in columns:
            if not cls.IDENTIFIER_PATTERN.match(col):
                raise ValueError(f"Invalid column name: {col}")
        return True
    
    @classmethod
    def validate_fecha(cls, fecha: str) -> bool:
        """Validate date format (YYYY-MM-DD)"""
        try:
            datetime.strptime(fecha, '%Y-%m-%d')
            return True
        except ValueError:
            raise ValueError(f"Invalid date format: {fecha}")
    
    @classmethod
    def scan_sql_for_dangerous_keywords(cls, sql: str) -> bool:
        """Scan SQL for dangerous keywords"""
        sql_lower = sql.lower()
        
        for keyword in cls.FORBIDDEN_KEYWORDS:
            if keyword in sql_lower:
                raise ValueError(f"Forbidden SQL keyword detected: {keyword}")
        
        return True
    
    @classmethod
    def validate_config(cls, config: Dict[str, Any]) -> bool:
        """Comprehensive config validation"""
        # Validate table name
        cls.validate_table_name(config['table_name'])
        
        # Validate fecha
        cls.validate_fecha(config['fecha'])
        
        # Validate column names in standard tests
        tests = config.get('tests', {})
        
        if 'completeness' in tests:
            cls.validate_column_names(tests['completeness'])
        
        if 'uniqueness' in tests:
            for key_combo in tests['uniqueness']:
                cls.validate_column_names(key_combo)
        
        # Validate custom SQL
        for custom_test in tests.get('custom_sql', []):
            if 'sql' in custom_test:
                cls.scan_sql_for_dangerous_keywords(custom_test['sql'])
        
        return True
```
