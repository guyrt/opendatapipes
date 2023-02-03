import argparse
import os

import pyspark.sql.functions as F
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from common import with_lower_case, read_folder

sc = SparkSession.builder \
            .appName("fecDeltaSH") \
            .getOrCreate()

parser = argparse.ArgumentParser()
parser.add_argument("--unzipped_fec_files", type=str)
parser.add_argument("--delta_uri", type=str)
parser.add_argument("--forms_done_gate", type=str)  # gate only - not used.
args = parser.parse_args()

unzipped_fec_folder = args.unzipped_fec_files
delta_uri = args.delta_uri


print(f"Running on {unzipped_fec_folder}")


def join_to_forms(df : DataFrame, df_forms : DataFrame):
    """Join DataFrame df to the forms DataFrame"""
    cond = [df['filer_committee_id_number'] == df_forms['filer_committee_id_number_formdf']]
    dfj = df.join(df_forms, cond, 'leftouter')
    dfj.cache()
    assert df.count() == dfj.count()
    return dfj


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
    dfsh = read_folder(sc, unzipped_fec_folder, "SH*")
except AnalysisException:
    import sys
    sys.exit(0)

dfshj = join_to_forms(dfsh, filers_df)
nulls_remain = dfshj.filter(F.col('upload_date_formdf').isNull())
print(nulls_remain.count())

dfshj = dfshj.cache()
dfshj = add_partitions(dfshj, 'expenditure_date')
for col_to_lower in ['payee_organization_name', 'payee_last_name', 'payee_first_name', 'payee_middle_name', 'payee_prefix', 'payee_suffix', 'payee_street_1', 'payee_street_2', 'payee_city', 'payee_state', 'payee_zip', 'memo_code', 'memo_textdescription']:
    dfshj = with_lower_case(dfshj, col_to_lower)

# enforce no duplicates
w2 = Window.partitionBy("filer_committee_id_number", "transaction_id_number", "YEAR", "MONTH").orderBy(F.col("upload_date").desc())
dfshj = dfshj.withColumn("__row__", F.row_number().over(w2)) \
  .filter(F.col("__row__") == 1).drop("__row__")


dfshj = dfshj.withColumn("original_file_formdf", F.lit(""))
# these random cols are a nuisance
dfshj = dfshj.drop("_only").drop("_only_copy")
dfshj.printSchema()


# Define Forms table
sc.sql(f"""
CREATE TABLE IF NOT EXISTS SH (
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
    accountevent_identifier STRING,
    expenditure_date STRING,
    nonfed_amount STRING,
    federal_share STRING,
    nonfederal_share STRING,
    activityevent_total_ytd STRING,
    expenditure_purpose_descrip STRING,
    category_code STRING,
    yesno_activity_is_direct_fundraising STRING,
    yesno_activity_is_an_exempt_activity STRING,
    yesno_activity_is_direct_candidate_support STRING,
    yesno_activity_is_public_communications_referring_only_to_party_made_by_pac STRING,
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
LOCATION '{os.path.join(delta_uri, "SH")}'
""")

base_table = DeltaTable.forPath(sc, os.path.join(delta_uri, 'SH'))
base_table.toDF().printSchema()
base_table.alias('target').merge(
    dfshj.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id_number == updates.transaction_id_number AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

