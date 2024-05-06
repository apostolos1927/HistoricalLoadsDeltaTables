# Databricks notebook source
dept = [(1,"Finance",10,20240429),(2,"Marketing",20,20240429),(3,"Sales",30,20240429),(4,"IT",40,20240426),(5,"Accounting",33,20240425),(5,"Accounting",34,20240423),(5,"Accounting",36,20240422),(5,"Accounting",37,20240421)]
rdd = sc.parallelize(dept)
columns = ["ID","DEPARTMENT","VALUE", "DateKey"]
df_rdd = rdd.toDF(columns)
df_rdd.show(truncate=False)

# COMMAND ----------

dept = [(6,"Finance11155",220,20240404),(7,"Marketin333g1uuu223",696,20240413)]
rdd = sc.parallelize(dept)
columns = ["ID","DEPARTMENT","VALUE", "DateKey"]
df_rdd = rdd.toDF(columns)
df_rdd.show(truncate=False)

# COMMAND ----------

df_rdd.write.mode("append").format('delta').partitionBy('DateKey').save("/mnt/demo/source_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/demo/source_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/mnt/demo/source_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/demo/log_dir1`

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/mnt/demo/log_dir1` where version>=0

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/mnt/demo/target_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY delta.`/mnt/demo/target_data`

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM delta.`/mnt/demo/target_data` WHERE ID>=5

# COMMAND ----------

import json
import os
from delta import DeltaTable
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType,StringType


class Merge:
    def __init__(self,schema: Dict[str, str],primary_key: List[str],partition_key: str,output_dir: str):
        self.schema = schema
        self.spark = spark
        self.primary_key = primary_key
        self.partition_key = partition_key
        self.output_dir = output_dir

    def _overwrite_output_schema(self) -> None:
        df = self.spark.createDataFrame([], schema=self.schema)
        df.write.format("delta").option("overwriteSchema", "True").partitionBy(self.partition_key).mode("append").save(self.output_dir)

    def _merge_condition(self, target_table: str = "target", source_table: str = "source") -> str:
        conditions = [f"{target_table}.{column} = {source_table}.{column}" for column in self.primary_key]
        conditions_string = " AND ".join(conditions)
        return conditions_string

    def _when_matched(self, columns: List[str], target_table: str = "target", source_table: str = "source") -> str:
        columns = [f"{target_table}.{column} = {source_table}.{column}" for column in columns if column not in self.primary_key]
        conditions_string = ", ".join(columns)
        return conditions_string

    def _when_not_matched(self, columns: List[str], source_table: str = "source") -> str:
        table_columns = ", ".join(columns)
        values = ", ".join([f"{source_table}.{column}" for column in columns])
        statement = f"({table_columns}) VALUES ({values})"
        return statement

    def execute(self, df: DataFrame) -> None:
        #if there is no target table the first time then create an empty dataframe
        if not DeltaTable.isDeltaTable(spark, self.output_dir):
            self._overwrite_output_schema()
        #load target table
        df_target = spark.read.format("delta").load(self.output_dir)

        #create temp views for merge statement
        df.createOrReplaceTempView("source")
        df_target.createOrReplaceTempView("target")
        
        columns = [i["name"] for i in df.schema.jsonValue()["fields"]]
        
        print(f"""
            MERGE INTO target
            USING source
            ON {self._merge_condition()}
            WHEN MATCHED THEN
                UPDATE SET {self._when_matched(columns)}
            WHEN NOT MATCHED
                THEN INSERT {self._when_not_matched(columns)}
            """)
        spark.sql(
            f"""
            MERGE INTO target
            USING source
            ON {self._merge_condition()}
            WHEN MATCHED THEN
                UPDATE SET {self._when_matched(columns)}
            WHEN NOT MATCHED
                THEN INSERT {self._when_not_matched(columns)}
            """
        )


class Changes:
    """
    Determine changes that happen on a delta table to only process changed data.
    The purpose of this class is to output a list of partition values (DateKey, WeekKey, etc.)
    that have changed data.
    """

    log_schema = StructType(
        [StructField("Version", IntegerType()), StructField("EventDateTime", TimestampType())]
    )
    determine_modes = ["merge", "append"]  ##add "replaceWhere" if you want to implement partition exchange

    def __init__(self, input_dir: str, log_dir: str, partition_key: list = None) -> None:

        self.input_dir = input_dir
        self.log_dir = log_dir
        self.changed_partition_keys: List[int] = []
        self.partition_key = partition_key

        # Retrieve the current version of the source delta table
        self.new_delta_version = (
            spark.sql(f"DESCRIBE HISTORY delta.`{self.input_dir}`")
            .where((col("operation") == "WRITE") | (col("operation") == "MERGE"))
            .agg(spark_max(col("version")).alias("MaxVersion"))
            .first()["MaxVersion"]
        )

        # Current version of the table in the log directory. Keep track of changes in the source table
        try:
            self.table_version = (
                spark.read.format("delta")
                .load(self.log_dir)
                .orderBy(col("EventDateTime").desc())
                .first()["Version"]
            )
        except Exception:
            # No log entry means retrieve the entire delta table.
            self.table_version = -1

        if self.new_delta_version < self.table_version:
            # In case of a table deletion, delta versioning starts at 1 again
            self.table_version = -1

    def _get_all_partitions(self) -> list:
        return [spark.read.format("delta").load(self.input_dir).select(self.partition_key).distinct().collect()]

    def _generate_path(self, n: str) -> str:
        """For a given delta version, generate the file name for the commit file."""
        return os.path.join(self.input_dir, "_delta_log/", str(n).zfill(20) + ".json")

    def _determine_merge_append(self) -> list:
        if self.table_version == -1:
            return self._get_all_partitions()
        else:
            new_versions = [
                i[0]
                for i in spark.sql(f"DESCRIBE HISTORY delta.`{self.input_dir}`")
                .where(col("version") > self.table_version)
                .where(
                    (col("operation") == "WRITE")
                    | (col("operation") == "MERGE")
                    | (col("operation") == "Append")
                )
                .select(col("version"))
                .distinct()
                .collect()
            ]

            if len(new_versions) > 0:
                # get the delta log files that show the delta changes
                # these delta log files contain the changed partitions
                
                delta_log_files = [self._generate_path(version) for version in new_versions]
                df_delta_log_files = spark.read.json(delta_log_files)
                columns_list = df_delta_log_files.columns
                
                # return the partition values of changed files
                if "add" in columns_list:
                    return (
                        [df_delta_log_files
                        .where("add is not null")
                        .where(f"add.partitionValues.{self.partition_key[0]} IS NOT NULL")
                        .select([f"add.partitionValues.{i}" for i in self.partition_key])
                        .distinct()
                        .collect()]
                    )
                else:
                    return []
            else:
                return []

    def determine(self, load_from: int = None, mode: str = "merge") -> list:
        """
        Determine all the partition values that have changed.
        """
    
        if isinstance(load_from, int):
            load_from = [str(load_from)]

        if mode not in self.determine_modes:
            raise ValueError(f"param mode should be one of the following values: {self.determine_modes}")

        if mode in ["append", "merge"]:
            changed_partitions = [i for i in self._determine_merge_append() if "".join([str(v) for v in i]) >= "".join(load_from)]
            self.changed_partition_keys = [v for i in changed_partitions for v in i ]
            print('changed_partition_keys',self.changed_partition_keys)
        return self.changed_partition_keys

    def create_log_record(self) -> None:
        df = spark.createDataFrame([[self.new_delta_version, datetime.now()]], self.log_schema)
        df.write.format("delta").mode("append").save(self.log_dir)



if __name__=="__main__":
    input_dir = "/mnt/demo/source_data"
    log_dir = "/mnt/demo/log_dir1"
    data_changes_partition_key = ["DateKey"]
    output_dir_primary_key = ["ID"]
    output_dir_partition_key = "DateKey"
    output_dir ="/mnt/demo/target_data"
    data_changes_mode="merge"
    source_schema = StructType(
            [StructField("ID", IntegerType()), StructField("DEPARTMENT", StringType()),StructField("VALUE", IntegerType()),StructField("DateKey",IntegerType())]
        )
    
    #Create a Data Changes object with the input params
    data_changes_input_dir1 = Changes(input_dir=input_dir, log_dir=log_dir, partition_key=data_changes_partition_key)
    #call the determine function to get the changed partitions
    partitions_input_dir1 = data_changes_input_dir1.determine(mode=data_changes_mode, load_from=20230101)
    print('partitions to load',[part[0] for part in partitions_input_dir1])
    
    #load source data 
    df = spark.read.format('delta').load(input_dir).where(col("DateKey").isin([part[0] for part in partitions_input_dir1]))
    
    #create Merge object
    merge = Merge(
                    schema=source_schema,
                    primary_key=output_dir_primary_key,
                    partition_key=output_dir_partition_key,
                    output_dir=output_dir
                )
    
    #Merge source data to the target table
    merge.execute(df)
    
    #create log entry
    data_changes_input_dir1.create_log_record()

