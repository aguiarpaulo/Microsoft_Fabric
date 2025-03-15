#!/usr/bin/env python
# coding: utf-8

# ## ingest
# 
# New notebook

# In[33]:


from pyspark.sql.functions import input_file_name,lit
from datetime import datetime 


# In[34]:


import csv
import glob
path = "abfss://83366a8b-c32f-43e5-a490-de40e3116cf1@onelake.dfs.fabric.microsoft.com/5d40bff3-23c7-4038-8309-c91560d89948/Files" 
all_files = glob.glob(path + "*.csv")

print(all_files)


# In[35]:


from pyspark.sql.functions import lit, current_timestamp
import pandas as pd

# Substitua o caminho pelo caminho real da sua pasta no Fabric
path = "abfss://83366a8b-c32f-43e5-a490-de40e3116cf1@onelake.dfs.fabric.microsoft.com/5d40bff3-23c7-4038-8309-c91560d89948/Files/*.csv"  # Atualize com o caminho copiado do Fabric
df = spark.read.option("header", True).csv(path)
df.show()


# In[36]:


df_filtered = df. select("Nome", "Idade", "Email")


# In[37]:


# Add metadata
df = df_filtered.withColumn("FileName", input_file_name()) \
        .withColumn("ProcessedTimestamp", lit(datetime.now().isoformat()))
df.show()


# In[39]:


# saving in a table
lakehouse_path = "abfss://83366a8b-c32f-43e5-a490-de40e3116cf1@onelake.dfs.fabric.microsoft.com/5d40bff3-23c7-4038-8309-c91560d89948/Tables/"
df.write.format("delta").mode("overwrite").save(lakehouse_path)


# In[44]:


lakehouse_path_table = "abfss://83366a8b-c32f-43e5-a490-de40e3116cf1@onelake.dfs.fabric.microsoft.com/5d40bff3-23c7-4038-8309-c91560d89948/Tables/extract"

# Register as SQL query
df.createOrReplaceTempView("TempViewExtract")

# Query the data
df_temp = spark.sql("SELECT * FROM TempViewExtract").show()

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(lakehouse_path_table)


# In[ ]:




