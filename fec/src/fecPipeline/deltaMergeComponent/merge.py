import argparse
import os

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.utils import AnalysisException
from pyspark.sql.window import Window

sc = SparkSession.builder \
            .master("local[4]") \
            .appName("fecDelta") \
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

filers_df = read_f_files(unzipped_fec_folder)

# orderBy essentially ranks F3X last b/c I've observed it being worse in the very few cases of overlap with F3X and F3.
window = Window.partitionBy("filer_committee_id_number_formdf").orderBy("clean_linetype_formdf")
filers_df = filers_df.withColumn('__rank__', F.rank().over(window)).filter(F.col('__rank__') == 1).drop('__rank__')

# Make sure there are note dupes on the eventual join key.
df_forms_counts = filers_df.groupBy('filer_committee_id_number_formdf').count().agg({'count': 'max'}).collect()
assert df_forms_counts[0].__getitem__('max(count)') == 1, "No duplicate committees b/c this is a join key"

filer_df_final = filers_df.select([F.col(c).alias(c.replace("_formdf", "")) for c in filers_df.columns])

# Define Forms table
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
LOCATION '{os.path.join(delta_uri, "AllForms")}'
""")

base_table = DeltaTable.forPath(sc, f'{delta_uri}/AllForms')
base_table.alias('target').merge(
    filer_df_final.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# SA Delta upload

# These come from a terminated account and can be found in Form 4.

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
    original_file_formdf STRING,
    earmarks STRING,
    YEAR STRING,
    MONTH STRING,
    COMMITTEE_PARTITION STRING )
USING DELTA
PARTITIONED BY (YEAR, MONTH, COMMITTEE_PARTITION)
LOCATION '{os.path.join(delta_uri, "SA")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SA'))
base_table.alias('target').merge(
    dfsaj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id == updates.transaction_id AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

