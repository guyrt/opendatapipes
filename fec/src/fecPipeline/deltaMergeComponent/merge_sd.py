import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sc = SparkSession.builder \
            .appName("fecDeltaSD") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--delta_uri", type=str)
parser.add_argument("--forms_done_gate", type=str)  # gate only - not used.
args = parser.parse_args()

unzipped_fec_folder = args.unzipped_fec_files
delta_uri = args.delta_uri


print(f"Running on {unzipped_fec_folder}")


def read_folder(base_uri, folder_name):
    full_uri = os.path.join(base_uri, folder_name)
    df = sc.read \
        .option("mergeSchema", "True") \
        .load(f'{full_uri}/*.parquet', format='parquet')
    return df


def join_to_forms(df : DataFrame, df_forms : DataFrame):
    """Join DataFrame df to the forms DataFrame"""
    cond = [df['filer_committee_id_number'] == df_forms['filer_committee_id_number_formdf']]
    dfj = df.join(df_forms, cond, 'leftouter')
    dfj.cache()
    assert df.count() == dfj.count()
    return dfj


def with_lower_case(df, col_name, tmp_col_name='_tmp'):
    raw_cols = df.columns
    return df.withColumn(tmp_col_name, F.lower(F.col(col_name)))\
                .drop(col_name)\
                .withColumnRenamed(tmp_col_name, col_name)\
                .select(*raw_cols)  # reorder to original


def add_partitions(df, col_to_prefix_name):
    # SD has no date column (it's tracking loans) so partition on hash.
    return df.withColumn('COMMITTEE_PREFIX', F.substring(col_to_prefix_name, 1, 4))


###
# Start of script
###

# Define Forms table
forms_path = os.path.join(delta_uri, "AllForms")
print(f"AllForms path: {forms_path}")

base_table = DeltaTable.forPath(sc, forms_path)
filers_df = base_table.toDF()
filers_df.printSchema()

# convert all column names
for col in filers_df.columns:
    filers_df = filers_df.withColumnRenamed(col, f"{col}_formdf")

dfsd = read_folder(unzipped_fec_folder, "SD")
dfsdj = join_to_forms(dfsd, filers_df)
nulls_remain = dfsdj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())

dfsdj = dfsdj.cache()
dfsdj = add_partitions(dfsdj, 'filer_committee_id_number')
for col_to_lower in [ 'creditor_organization_name', 'creditor_last_name', 'creditor_first_name', 'creditor_middle_name', 'creditor_prefix', 'creditor_suffix', 'creditor_street_1', 'creditor_street_2', 'creditor_city', 'creditor_state', 'creditor_zip', 'purpose_of_debt_or_obligation']:
    dfsdj = with_lower_case(dfsdj, col_to_lower)

# enforce no duplicates
w2 = Window.partitionBy("filer_committee_id_number", "transaction_id_number", "COMMITTEE_PREFIX").orderBy(F.col("upload_date").desc())
dfsdj = dfsdj.withColumn("__row__", F.row_number().over(w2)) \
  .filter(F.col("__row__") == 1).drop("__row__")


dfsdj = dfsdj.withColumn("original_file_formdf", F.lit(""))
dfsdj.printSchema()

# Define Forms table
sc.sql(f"""
CREATE TABLE IF NOT EXISTS SD (
    clean_linetype STRING,
    upload_date STRING,
    form_type STRING,
    filer_committee_id_number STRING,
    transaction_id_number STRING,
    entity_type STRING,
    creditor_organization_name STRING,
    creditor_last_name STRING,
    creditor_first_name STRING,
    creditor_middle_name STRING,
    creditor_prefix STRING,
    creditor_suffix STRING,
    creditor_street_1 STRING,
    creditor_street_2 STRING,
    creditor_city STRING,
    creditor_state STRING,
    creditor_zip STRING,
    purpose_of_debt_or_obligation STRING,
    beginning_balance_this_period STRING,
    incurred_amount_this_period STRING,
    payment_amount_this_period STRING,
    balance_at_close_this_period STRING,
    clean_linetype_formdf STRING,
    upload_date_formdf STRING,
    filer_committee_id_number_formdf STRING,
    committee_name_formdf STRING,
    street_1_formdf STRING,
    street_2_formdf STRING,
    city_formdf STRING,
    state_formdf STRING,
    zip_formdf STRING,
    report_code_formdf STRING,
    date_of_election_formdf STRING,
    state_of_election_formdf STRING,
    date_signed_formdf STRING,
    original_file_formdf STRING,
    filename STRING,
    COMMITTEE_PREFIX STRING )
USING DELTA
PARTITIONED BY (COMMITTEE_PREFIX)
LOCATION '{os.path.join(delta_uri, "SD")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SD'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfsdj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id_number == updates.transaction_id_number AND target.COMMITTEE_PREFIX == updates.COMMITTEE_PREFIX" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

