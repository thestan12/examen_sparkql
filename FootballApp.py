from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark import *
from pyspark.sql.types import BooleanType
import pyspark.sql.functions as F
from pyspark.sql import *
from pyspark.sql.functions import *

from pandas import *
from pyspark import *


spark = SparkSession \
    .builder \
    .getOrCreate()

df_all = spark.read.option("header",True).csv("./data/df_matches.csv")

df_renamed = df_all.withColumnRenamed("X4", "match").withColumnRenamed("X6", "competition")

df_selected = df_renamed.select("match", "competition", "adversaire", "score_france", "score_adversaire", "penalty_france", "penalty_adversaire", "date")



df_selected = df_selected.fillna("0", subset=['penalty_france', 'penalty_adversaire'])
df_selected = df_selected.na.replace('NA', '0', 'penalty_france')
df_selected = df_selected.na.replace('NA', '0', 'penalty_adversaire')
df_selected.withColumn('penalty_france', regexp_replace('penalty_france', 'NA', '0'))
df_selected.withColumn('penalty_france', regexp_replace('penalty_adversaire', 'NA', '0'))

# df_selected.na.fill({'penalty_france': '10', 'penalty_france': '0'})

df_selected = df_selected.where(F.col("date") > "1980-0-0")


extract_title_udf = F.udf(lambda name, adv: "france" in (name.split("-")[0]).lower() or adv.lower().replace(" ", "") in (name.split("-")[0]).lower(), BooleanType())

df_selected = df_selected.withColumn('isDomicile', extract_title_udf(df_selected.match, df_selected.adversaire))

agg_by_class = (df_selected
                .groupBy("adversaire")
                .agg(
    F.avg(df_selected.score_france).alias("avg_score_france"),
    F.avg(df_selected.score_adversaire).alias("avg_score_adversaire"),
    F.count(df_selected.score_france).alias("count_nb_match"),
    F.max(df_selected.penalty_france).alias("max_france_penality"),
    (F.sum(df_selected.penalty_france) - F.sum(df_selected.penalty_adversaire)).alias("penalityfrance_moins_penalityadversaire"),
))

agg_by_class.show()

response_file_name = "./response/stats.parquet.csv"
agg_by_class.toPandas().to_csv(response_file_name)
df_all = spark.read.option("header",True).csv(response_file_name)

df_all = df_all.join(df_selected, df_selected.adversaire == df_all.adversaire, 'inner')

response_join_file_name = "./response/stats.parquet.csv"
df_all.toPandas().to_csv(response_join_file_name)
df_all.show()
# F.sum(F.col("IsUnemployed").cast("long")
#     cnt_cond( "Coupe du monde" in F.col('competition')).alias('y_cnt'),

# .withColumn("perc", F.sum("penalty_france").over(Window.partitionBy("adversaire")) - F.sum("penalty_adversaire").over(Window.partitionBy("adversaire")))