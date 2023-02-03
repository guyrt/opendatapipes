import os
import pyspark.sql.functions as F


def with_lower_case(df, col_name, tmp_col_name='_tmp'):
    raw_cols = df.columns
    return df.withColumn(tmp_col_name, F.lower(F.col(col_name)))\
                .drop(col_name)\
                .withColumnRenamed(tmp_col_name, col_name)\
                .select(*raw_cols)  # reorder to original


def read_folder(spark_context, base_uri, folder_name):
    full_uri = os.path.join(base_uri, folder_name)
    df = spark_context.read \
        .option("mergeSchema", "True") \
        .load(f'{full_uri}/*.parquet', format='parquet')
    return df


def add_date_partitions(df, date_col_name):
    # partition to year/month. later consider a partition to candidate if you want fast lookups.
    return df.withColumn('YEAR', F.substring(date_col_name, 1, 4)) \
             .withColumn('MONTH', F.substring(date_col_name, 5, 2))
