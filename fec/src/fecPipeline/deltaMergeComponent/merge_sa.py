import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

sc = SparkSession.builder \
            .appName("fecDeltaSA") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--delta_uri", type=str)
parser.add_argument("--forms_done_gate", type=str)  # gate only - not used.
parser.add_argument("--side_effect_done_file", type=str)
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


def extract_earmarks(df):
    # WinRed
    winred_col = 'contribution_purpose_descrip'
    actblue_col = 'memo_code'

    df = df.withColumn("__winred__", F.regexp_extract(F.col(winred_col), "\((c\d+)\)", 1))
    df = df.withColumn("__actblue__", F.regexp_extract(F.col(actblue_col), "\((c\d+)\)", 1))

    df = df.withColumn("earmarks", F.when(F.col("__winred__") != '', F.col("__winred__")).when(F.col("__actblue__") != '', F.col("__actblue__")).otherwise(''))
    df = df.drop("__winred__").drop("__actblue__")
    df = df.withColumn("earmarks", F.upper(F.col("earmarks")))
    return df


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

dfsa = read_folder(unzipped_fec_folder, "SA")
dfsaj = join_to_forms(dfsa, filers_df)
nulls_remain = dfsaj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())
nulls_remain.select('form_type', 'filer_committee_id_number').head(5)

dfsaj = dfsaj.cache()
dfsaj = extract_earmarks(dfsaj)
dfsaj = add_partitions(dfsaj, 'contribution_date')
for col_to_lower in ['contributor_organization_name', 'contributor_last_name', 'contributor_first_name', 'contributor_middle_name', 'contributor_prefix', 'contributor_suffix', 'contributor_street_1', 'contributor_street_2', 'contributor_city', 'contributor_state', 'contributor_zip', 'contribution_purpose_descrip', 'contributor_employer', 'contributor_occupation', 'donor_committee_fec_id', 'donor_committee_name', 'donor_candidate_fec_id', 'donor_candidate_last_name', 'donor_candidate_first_name', 'donor_candidate_middle_name', 'donor_candidate_prefix', 'donor_candidate_suffix', 'donor_candidate_office', 'donor_candidate_state', 'donor_candidate_district', 'conduit_name', 'conduit_street1', 'conduit_street1_copy', 'conduit_street2', 'conduit_city', 'conduit_state', 'conduit_zip', 'memo_code', 'memo_textdescription']:
    dfsaj = with_lower_case(dfsaj, col_to_lower)

# enforce no duplicates
w2 = Window.partitionBy("filer_committee_id_number", "transaction_id", "YEAR", "MONTH").orderBy(F.col("upload_date").desc())
dfsaj = dfsaj.withColumn("__row__", F.row_number().over(w2)) \
  .filter(F.col("__row__") == 1).drop("__row__")

dfsaj = dfsaj.withColumn("original_file_formdf", F.lit(""))
dfsaj.printSchema()

# Define Forms table
sc.sql(f"""
CREATE TABLE IF NOT EXISTS SA (
    clean_linetype STRING,
    upload_date STRING,
    form_type STRING,
    filer_committee_id_number STRING,
    transaction_id STRING,
    back_reference_tran_id_number STRING,
    back_reference_sched_name STRING,
    entity_type STRING,
    contributor_organization_name STRING,
    contributor_last_name STRING,
    contributor_first_name STRING,
    contributor_middle_name STRING,
    contributor_prefix STRING,
    contributor_suffix STRING,
    contributor_street_1 STRING,
    contributor_street_2 STRING,
    contributor_city STRING,
    contributor_state STRING,
    contributor_zip STRING,
    election_code STRING,
    election_other_description STRING,
    contribution_date STRING,
    contribution_amount_f3l_bundled STRING,
    annual_bundled STRING,
    contribution_purpose_descrip STRING,
    contributor_employer STRING,
    contributor_occupation STRING,
    donor_committee_fec_id STRING,
    donor_committee_name STRING,
    donor_candidate_fec_id STRING,
    donor_candidate_last_name STRING,
    donor_candidate_first_name STRING,
    donor_candidate_middle_name STRING,
    donor_candidate_prefix STRING,
    donor_candidate_suffix STRING,
    donor_candidate_office STRING,
    donor_candidate_state STRING,
    donor_candidate_district STRING,
    conduit_name STRING,
    conduit_street1 STRING,
    conduit_street1_copy STRING,
    conduit_street2 STRING,
    conduit_city STRING,
    conduit_state STRING,
    conduit_zip STRING,
    memo_code STRING,
    memo_textdescription STRING,
    reference_to_si_or_sl_system_code_that_identifies_the_account STRING,
    clean_linetype_formdf STRING,
    filename STRING,
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
    earmarks STRING,
    YEAR STRING,
    MONTH STRING)
USING DELTA
PARTITIONED BY (YEAR, MONTH)
LOCATION '{os.path.join(delta_uri, "SA")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SA'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfsaj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id == updates.transaction_id AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

