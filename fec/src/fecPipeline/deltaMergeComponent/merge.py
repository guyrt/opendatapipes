import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import pyspark.sql.functions as F

sc = SparkSession.builder \
            .master("local[4]") \
            .appName("fecDelta") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
args = parser.parse_args()

unzipped_fec_folder = args.unzipped_fec_files

print(f"Running on {unzipped_fec_folder}")

def clean_filer(df_filer, origin):

    df_filer = df_filer.select(
        'clean_linetype', 
        'upload_date', 
        'filer_committee_id_number', 
        'committee_name', 
        'street_1', 
        'street_2', 
        'city', 
        'state', 
        'zip', 
        'report_code', 
        'date_of_election', 
        'state_of_election',
        'date_signed',
        F.lit(origin).alias('original_file'))

    df_filer = df_filer.withColumn('date_signed_int', df_filer['date_signed'].cast(IntegerType()))

    df_filer_window = Window.partitionBy("filer_committee_id_number").orderBy(F.col("date_signed_int").desc())
    df_filer_final = df_filer.withColumn("row", F.row_number().over(df_filer_window)) \
                              .filter(F.col("row") == 1)

    df_filer_final = df_filer_final.drop("row") \
                            .drop('date_signed_int')

    df_filer_final = df_filer_final.select(*(F.col(x).alias(x + '_formdf') for x in df_filer_final.columns))
    return df_filer_final


def read_f_files(base_uri):
    """Read all of the F type files that we want to process."""
    suffixes = ['F3', 'F3P', 'F3X']
    dataframes = []
    for f_file_suffix in suffixes:
        full_uri = os.path.join(base_uri, f_file_suffix)
        try:
            df = sc.read \
                .option("mergeSchema", "True") \
                .load(f'{full_uri}/*.parquet', format='parquet')
            df = clean_filer(df, f_file_suffix.lower())
            dataframes.append(df)
        except AnalysisException:
            continue

    base_df = dataframes[0]
    for df in dataframes[1:]:
        base_df = base_df.union(df)

    return base_df


filers_df = read_f_files(unzipped_fec_folder)

print(filers_df.count())
