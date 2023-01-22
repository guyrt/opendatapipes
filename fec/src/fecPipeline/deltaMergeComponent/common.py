import pyspark.sql.functions as F

def with_lower_case(df, col_name, tmp_col_name='_tmp'):
    raw_cols = df.columns
    return df.withColumn(tmp_col_name, F.lower(F.col(col_name)))\
                .drop(col_name)\
                .withColumnRenamed(tmp_col_name, col_name)\
                .select(*raw_cols)  # reorder to original
