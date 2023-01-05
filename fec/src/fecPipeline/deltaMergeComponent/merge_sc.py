import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sc = SparkSession.builder \
            .appName("fecDeltaSC") \
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
    # SC has no date column (it's tracking loans) so partition on hash.
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

dfsc = read_folder(unzipped_fec_folder, "SC")
dfscj = join_to_forms(dfsc, filers_df)
nulls_remain = dfscj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())

dfscj = dfscj.cache()
dfscj = add_partitions(dfscj, 'filer_committee_id_number')
for col_to_lower in ['lender_organization_name', 'lender_last_name', 'lender_first_name', 'lender_middle_name', 'lender_prefix', 'lender_suffix', 'lender_street_1', 'lender_street_2', 'lender_city', 'lender_state', 'lender_zip',  'lender_committee_id_number', 'lender_candidate_id_number', 'lender_candidate_last_name', 'lender_candidate_first_name', 'lender_candidate_middle_nm', 'lender_candidate_prefix', 'lender_candidate_suffix', 'lender_candidate_office', 'lender_candidate_state', 'lender_candidate_district', 'memo_code', 'memo_textdescription', 'memo_textdescription_copy']:
    dfscj = with_lower_case(dfscj, col_to_lower)

# enforce no duplicates
w2 = Window.partitionBy("filer_committee_id_number", "transaction_id_number", "COMMITTEE_PREFIX").orderBy(F.col("upload_date").desc())
dfscj = dfscj.withColumn("__row__", F.row_number().over(w2)) \
  .filter(F.col("__row__") == 1).drop("__row__")

dfscj = dfscj.withColumn("original_file_formdf", F.lit(""))
dfscj = dfscj.withColumnRenamed("loan_interest_rate_%_terms", "loan_interest_rate_percent_terms")
dfscj.printSchema()

# Define Forms table
sc.sql(f"""
CREATE TABLE IF NOT EXISTS SC (
    clean_linetype STRING,
    upload_date STRING,
    form_type STRING,
    filer_committee_id_number STRING,
    transaction_id_number STRING,
    receipt_line_number STRING,
    entity_type STRING,
    lender_organization_name STRING,
    lender_last_name STRING,
    lender_first_name STRING,
    lender_middle_name STRING,
    lender_prefix STRING,
    lender_suffix STRING,
    lender_street_1 STRING,
    lender_street_2 STRING,
    lender_city STRING,
    lender_state STRING,
    lender_zip STRING,
    election_code STRING,
    election_other_description STRING,
    loan_amount_original STRING,
    loan_payment_to_date STRING,
    loan_balance STRING,
    loan_incurred_date_terms STRING,
    loan_due_date_terms STRING,
    loan_interest_rate_percent_terms STRING,
    yesno_secured STRING,
    yesno_personal_funds STRING,
    lender_committee_id_number STRING,
    lender_candidate_id_number STRING,
    lender_candidate_last_name STRING,
    lender_candidate_first_name STRING,
    lender_candidate_middle_nm STRING,
    lender_candidate_prefix STRING,
    lender_candidate_suffix STRING,
    lender_candidate_office STRING,
    lender_candidate_state STRING,
    lender_candidate_district STRING,
    memo_code STRING,
    memo_textdescription STRING,
    memo_textdescription_copy STRING,
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
LOCATION '{os.path.join(delta_uri, "SC")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SC'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfscj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id_number == updates.transaction_id_number AND target.COMMITTEE_PREFIX == updates.COMMITTEE_PREFIX" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

