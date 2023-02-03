import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window


sc = SparkSession.builder \
            .appName("fecDeltaForms") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--delta_uri", type=str)
args = parser.parse_args()

unzipped_fec_folder = args.unzipped_fec_files
delta_uri = args.delta_uri

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


###
# Start of script
###

filers_df = read_f_files(unzipped_fec_folder)

# orderBy essentially ranks F3X last b/c I've observed it being worse in the very few cases of overlap with F3X and F3.
window = Window.partitionBy("filer_committee_id_number_formdf").orderBy("clean_linetype_formdf")
filers_df = filers_df.withColumn('__rank__', F.rank().over(window)).filter(F.col('__rank__') == 1).drop('__rank__')

# Make sure there are note dupes on the eventual join key.
df_forms_counts = filers_df.groupBy('filer_committee_id_number_formdf').count().agg({'count': 'max'}).collect()
assert df_forms_counts[0].__getitem__('max(count)') == 1, "No duplicate committees b/c this is a join key"

filer_df_final = filers_df.select([F.col(c).alias(c.replace("_formdf", "")) for c in filers_df.columns])

# Define Forms table
forms_path = os.path.join(delta_uri, "AllForms")
print(f"AllForms path: {forms_path}")
sc.sql(f"""
CREATE TABLE IF NOT EXISTS AllForms (
    clean_linetype STRING, 
    upload_date STRING, 
    filer_committee_id_number STRING, 
    committee_name STRING, 
    street_1 STRING, 
    street_2 STRING, 
    city STRING, 
    state STRING, 
    zip STRING, 
    report_code STRING, 
    date_of_election STRING, 
    state_of_election STRING,
    date_signed STRING )
USING DELTA
LOCATION '/tables/AllForms'
""")

base_table = DeltaTable.forPath(sc, forms_path)
base_table.alias('target').merge(
    filer_df_final.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
