import argparse
import os

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

sc = SparkSession.builder \
            .appName("fecDeltaSE") \
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

try:
    dfsh = read_folder(unzipped_fec_folder, "SE")
except AnalysisException:
    import sys
    sys.exit(0)

dfshj = join_to_forms(dfsh, filers_df)
nulls_remain = dfshj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())

dfshj = dfshj.cache()
dfshj = add_partitions(dfshj, 'disbursement_date')

# these random cols are a nuisance
dfshj = dfshj.withColumnRenamed("supportoppose_code__", "supportoppose_code")
dfshj = dfshj.withColumnRenamed("election_other_description_", "election_other_description")
dfshj = dfshj.withColumnRenamed("payee_street__1", "payee_street_1")\
    .withColumnRenamed("payee_street__2", "payee_street_2")
                     
for col_to_lower in ['payee_organization_name', 'payee_last_name', 'payee_first_name', 'payee_middle_name', 'payee_prefix', 'payee_suffix', 'payee_street_1', 'payee_street_2', 'payee_city', 'payee_state', 'payee_zip', 'election_code', 'election_other_description', 't_d_per_electionoffice', 'expenditure_purpose_descrip', 'category_code', 'supportoppose_code', 'so_candidate_first_name', 'so_candinate_middle_name', 'so_candidate_prefix', 'so_candidate_suffix', 'so_candidate_office', 'so_candidate_district', 'so_candidate_state', 'completing_last_name', 'completing_first_name', 'completing_first_name_copy', 'completing_middle_name', 'completing_prefix', 'completing_suffix', 'memo_code', 'memo_textdescription']:
    dfshj = with_lower_case(dfshj, col_to_lower)

dfshj = dfshj.withColumn("original_file_formdf", F.lit(""))


dfshj.printSchema()


# Define Forms table
sc.sql(f"""
CREATE TABLE IF NOT EXISTS SE (
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
    dissemination_date STRING,
    expenditure_amount STRING,
    disbursement_date STRING,
    t_d_per_electionoffice STRING,
    expenditure_purpose_descrip STRING,
    category_code STRING,
    payee_cmtte_fec_id_number STRING,
    supportoppose_code STRING,
    so_candidate_id_number STRING,
    so_candidate_last_name STRING,
    so_candidate_first_name STRING,
    so_candinate_middle_name STRING,
    so_candidate_prefix STRING,
    so_candidate_suffix STRING,
    so_candidate_office STRING,
    so_candidate_district STRING,
    so_candidate_state STRING,
    completing_last_name STRING,
    completing_first_name STRING,
    completing_first_name_copy STRING,
    completing_middle_name STRING,
    completing_prefix STRING,
    completing_suffix STRING,
    date_signed STRING,
    memo_code STRING,
    memo_textdescription STRING,
    filename STRING,

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
LOCATION '{os.path.join(delta_uri, "SE")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SE'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfshj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id_number == updates.transaction_id_number AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

