import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sc = SparkSession.builder \
            .appName("fecDeltaSB") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--delta_uri", type=str)
parser.add_argument("--forms_done_gate", type=str)  # gate only - not used.
parser.add_argument("--side_effect_done_file", type=str)  # gate only
args = parser.parse_args()

unzipped_fec_folder = args.unzipped_fec_files
delta_uri = args.delta_uri
output_uri = args.side_effect_done_file


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


def add_partitions(df, date_col_name):
    # partition to year/month. later consider a partition to candidate if you want fast lookups.
    return df.withColumn('YEAR', F.substring(date_col_name, 1, 4)) \
             .withColumn('MONTH', F.substring(date_col_name, 5, 2))


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

dfsb = read_folder(unzipped_fec_folder, "SB")
dfsbj = join_to_forms(dfsb, filers_df)
nulls_remain = dfsbj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())

dfsbj = dfsbj.cache()
dfsbj = add_partitions(dfsbj, 'expenditure_date')
for col_to_lower in ['contributor_organization_name', 'contributor_last_name', 'contributor_first_name', 'contributor_middle_name', 'contributor_prefix', 'contributor_suffix', 'contributor_street_1', 'contributor_street_2', 'contributor_city', 'contributor_state', 'contributor_zip', 'contribution_purpose_descrip', 'contributor_employer', 'contributor_occupation', 'donor_committee_fec_id', 'donor_committee_name', 'donor_candidate_fec_id', 'donor_candidate_last_name', 'donor_candidate_first_name', 'donor_candidate_middle_name', 'donor_candidate_prefix', 'donor_candidate_suffix', 'donor_candidate_office', 'donor_candidate_state', 'donor_candidate_district', 'conduit_name', 'conduit_street1', 'conduit_street1_copy', 'conduit_street2', 'conduit_city', 'conduit_state', 'conduit_zip', 'memo_code', 'memo_textdescription']:
    dfsbj = with_lower_case(dfsbj, col_to_lower)

# enforce no duplicates
w2 = Window.partitionBy("filer_committee_id_number", "transaction_id", "YEAR", "MONTH").orderBy(F.col("upload_date").desc())
dfsbj = dfsbj.withColumn("__row__", F.row_number().over(w2)) \
  .filter(F.col("__row__") == 1).drop("__row__")

dfsbj = dfsbj.withColumn("original_file_formdf", F.lit(""))
dfsbj.printSchema()

# Define Forms table
sc.sql(f"""

    clean_linetype STRING,
    upload_date STRING,
    form_type STRING,
    filer_committee_id_number STRING,
    transaction_id_number STRING,
    back_reference_tran_id_number STRING,
    back_reference_sched_name STRING,
    entity_type STRING,
    payee_organization_name STRING,
    payee_last_name STRING,
    payee_first_name STRING,
    payee_middle_name STRING,
    payee_prefix STRING,
    payee_suffix STRING,
    payee_street_1 STRING,
    payee_street_2 STRING,
    payee_city STRING,
    payee_state STRING,
    payee_zip STRING,
    election_code STRING,
    election_other_description STRING,
    expenditure_date STRING,
    expenditure_amount_f3l_bundled STRING,
    annual_refunded_bundled_amt STRING,
    expenditure_purpose_descrip STRING,
    category_code STRING,
    beneficiary_committee_fec_id STRING,
    beneficiary_committee_name STRING,
    beneficiary_candidate_fec_id STRING,
    beneficiary_candidate_last_name STRING,
    beneficiary_candidate_first_name STRING,
    beneficiary_candidate_middle_name STRING,
    beneficiary_candidate_prefix STRING,
    beneficiary_candidate_suffix STRING,
    beneficiary_candidate_office STRING,
    beneficiary_candidate_state STRING,
    beneficiary_candidate_district STRING,
    conduit_name STRING,
    conduit_street_1 STRING,
    conduit_street_2 STRING,
    conduit_street_2_copy STRING,
    conduit_city STRING,
    conduit_state STRING,
    conduit_zip STRING,
    memo_code STRING,
    memo_textdescription STRING,
    reference_to_si_or_sl_system_code_that_identifies_the_account STRING,
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
    YEAR STRING,
    MONTH STRING )
USING DELTA
PARTITIONED BY (YEAR, MONTH)
LOCATION '{os.path.join(delta_uri, "SB")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SB'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfsbj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id == updates.transaction_id AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

