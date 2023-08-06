# The SA table has no earmarks col as of 2923-08-05. 
# This leaves all old data incorrect but recoverable.
# This job does a few things:
# 1) Alter the definition to add missing earmark col.
# 2) Create a new DF that has earmark correctly.
# 3) Use a giant merge step to correct old data.

import pyspark.sql.functions as F
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


blob_account_name = "fecblob"
blob_container_name = "rawzips"
from pyspark.sql import SparkSession

sc = SparkSession.builder.getOrCreate()

def extract_earmarks(df):
    winred_col = 'contribution_purpose_descrip'
    actblue_col = 'memo_code'

    df = df.withColumn("__winred__", F.regexp_extract(F.col(winred_col), "\((c\d+)\)", 1))
    df = df.withColumn("__actblue__", F.regexp_extract(F.col(actblue_col), "\((c\d+)\)", 1))

    df = df.withColumn("earmarks", F.when(F.col("__winred__") != '', F.col("__winred__")).when(F.col("__actblue__") != '', F.col("__actblue__")).otherwise(''))
    df = df.drop("__winred__").drop("__actblue__")
    df = df.withColumn("earmarks", F.upper(F.col("earmarks")))
    return df

# alter
sc.sql("""
ALTER TABLE SA ADD COLUMNS (earmarks STRING)
       """)


# get data
df = spark.read.table('SA')

df = extract_earmarks(df)


base_table = DeltaTable.forName(sc, 'SA')
base_table.toDF().printSchema()
base_table.alias('target').merge(
    df.alias('updates'), 
    "target.filer_committee_id_number == updates.filer_committee_id_number AND target.transaction_id == updates.transaction_id AND target.YEAR == updates.YEAR AND target.MONTH == updates.MONTH" ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()


