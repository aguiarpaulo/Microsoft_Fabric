#!/usr/bin/env python
# coding: utf-8

# ## Mastering_ingest
# 
# New notebook

# In[ ]:


path = "Files/"

df = spark.read.format("csv").option("header", "True").load(path)
df.write.mode("append").format("delta").saveAsTable("names_unified")

display(df)


# In[ ]:


url = "https://rawtraining.blob.core.windows.net/titanic/titanic.parquet"

df = spark.read.parquet(url)

display(df)


# In[5]:


from pyspark.sql import SparkSession

# Criação da SparkSession
spark = SparkSession.builder \
    .appName("LerParquetNoBlobStorage") \
    .getOrCreate()

# Configurações de acesso ao Blob Storage
spark.conf.set("spark.hadoop.fs.azure.account.key.rawtraining.blob.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2025-03-17T19:45:51Z&st=2025-03-17T11:45:51Z&spr=https&sig=0GDzD0ConH9XCeZ0XRUQtNS3fmFz7Mq8ErPEZyOgynU%3D")

# Caminho do arquivo Parquet no Blob Storage
blob_path = "wasb://titanic@rawtraining.blob.core.windows.net/titanic"

# Ler o arquivo Parquet
df = spark.read.parquet(blob_path)

# Mostrar as primeiras linhas do DataFrame
df.show()


# In[ ]:




