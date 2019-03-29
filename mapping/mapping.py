import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
import sys
import pyspark.sql.functions as f
spark=SparkSession.builder.appName("Mapping").getOrCreate()

mapping_pd=pd.read_excel("F:\spark\spark-2.1.2-bin-hadoop2.7\mapping\mapping.xlsx")
source_table=mapping_pd['source_table'].unique()[0]
source_columns_direct=mapping_pd[mapping_pd["mapping_type"]=="DIRECT"]["source_column"].tolist()
target_columns_direct=mapping_pd[mapping_pd["mapping_type"]=="DIRECT"]["target_column"].tolist()
direct_dic=dict((source,target) for source,target in zip(source_columns_direct,target_columns_direct))
#target_columns=mapping["target_column"]
target_table=mapping_pd['target_table'].unique()[0]
print("The source table is {} \nThe target table is {}".format(source_table,target_table))

source_columns_direct
target_columns_direct
try:
    assert(len(source_columns_direct)==len(target_columns_direct))
except AssertionError as e:
    print("Number of unique columns for source and target doesnt match",e.message)
    sys.exit(1)
	
source=spark.read.format('csv').load(source_table,header=True)
source.show()
target=spark.read.format('csv').load(target_table,header=True)
target.show()


#direct_dict
#target=target.withColumn("t_row_id",f.monotonically_increasing_id())
#source=source.withColumn("s_row_id",f.monotonically_increasing_id())


target=source
for s_col,t_col in direct_dic.items():
    target=target.withColumn(t_col,source[s_col])

derived=mapping_pd[mapping_pd["mapping_type"]=="DERIVED"]

for i in range(derived.shape[0]):
    sub_mapping=derived.iloc[i]["sub_mapping"]
    mapping=derived.iloc[i]["mapping"]
    print(sub_mapping)
    if sub_mapping=="FUNCTION":
        if mapping=="genuuid":
            target=target.withColumn(derived.iloc[i]["target_column"],f.monotonically_increasing_id())
    elif sub_mapping=="CONCATENATE":
        columns=derived.iloc[i]["mapping"].split(":")[0]
        sep=derived.iloc[i]["mapping"].split(":")[1].split(",")[1].strip()
        #print(columns.split(","))
        target=target.withColumn(derived.iloc[i]["target_column"],f.concat_ws(sep,*columns.split(",")))
        #target.show()
    elif sub_mapping=="JOIN":
        join_data=derived.iloc[i]["mapping"]
        tables=join_data.split("*")[0].split("#")[1].strip()
        lkp_data=spark.read.format('csv').load(tables,inferSchema=True,header=True)
        filters=join_data.split("*")[1].split("#")[1].strip()
        #print(filters)
        lkp_data=lkp_data.where(filters)
        #lkp_data.show()
        condition=join_data.split("*")[2].split("#")[1].strip()
        output_column=join_data.split("*")[3].split("#")[1].strip()
        target=target.alias("t").join(broadcast(lkp_data).alias("s"),how="left",on=f.col('t.'+condition.split("=")[0].strip())==f.col('s.'+condition.split("=")[1].strip()))#.select("t.*",output_column)
        print(derived.iloc[i]["target_column"])
        target=target.withColumnRenamed(output_column,derived.iloc[i]["target_column"])
        #target.show()
target.select(*mapping_pd["target_column"].tolist()).show()

lkp_data.show()

lkp_data=lkp_data.filter(filters)
lkp_data.show()

