#!/usr/bin/env python
# coding: utf-8

# ## metadata
# 
# New notebook

# In[1]:


import json
path_prefix_file_api = "/lakehouse/default/Files/metadata"
path_prefix_spark = "Files/metadata"
config_file_name = "config.json"


# Reading configuration file
with open(f"{path_prefix_file_api}/{config_file_name}","r") as file:
    config_list = json.load(file)
print(config_list)


# In[2]:


i = 1
for config in config_list:
    print(f"**CONFIG {str(i)}**")
    print(f"src_file:\t{config['src_file']}")
    print(f"dst_table:\t{config['dst_table']}")
    print(f"write_mode:\t{config['write_mode']}")
    print("*************************************")
    i += 1


# In[3]:


# Read and display data
i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t {str(i)}/{str(len_config_list)}")
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['write_mode']}")
    df_csv = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/source/{config['src_file']}")
    display(df_csv)
    print("*********************************************************")
    i += 1


# In[4]:


# Write data to lakehouse table
i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE:\t{str(i)}/{str(len_config_list)}")
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/source/{config['src_file']}")
    df_raw.write.mode(config['write_mode']).saveAsTable(f"{config['dst_table']}")
    print("*************************************")
    i += 1


# In[14]:


# Logic to functions for a cleaner and modular code
def read_notebook_config(path):
    with open(path, "r") as file:
        config_list = json.load(file)
    return config_list

def write_csv_file_to_lh_table(config):
    print(f"SOURCE FILE NAME:\t{config['src_file']}")
    print(f"DESTINATION TABLE:\t{config['dst_table']}")
    print(f"USING WRITE MODE:\t{config['write_mode']}")
    df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/source/{config['src_file']}")
    df_raw.write.mode(config['write_mode']).saveAsTable(f"{config['dst_table']}")
    print("*************************************")


# In[15]:


# Funtion execution

config_path = f"{path_prefix_file_api}/{config_file_name}"
config_list = read_notebook_config(config_path)

i = 1
len_config_list = len(config_list)
for config in config_list:
    print(f"PROCESSING FILE: \t{str(i)}/{str(len_config_list)}")
    write_csv_file_to_lh_table(config)
    i += 1


# In[20]:


# Funtions for parallel processing using concurrent.futures library
from concurrent.futures import ThreadPoolExecutor, as_completed

def read_notebook_config(path):
    with open(path,"r") as file:
        config_list = json.load(file)
    return config_list

def write_csv_file_to_lh_table_parallel(config):
    try:
        df_raw = spark.read.format("csv").option("header","true").load(f"{path_prefix_spark}/source/{config['src_file']}")
        df_raw.write.mode(config['write_mode']).saveAsTable(f"{config['dst_table']}")
        return f"PROCESSING OK FOR FILE: \t{config['src_file']}"
    except Exception as e:
        raise ValueError(f"AN ERROR OCURRED WHILE PROCESSING FILE: {config['src_file']}: (e)")

def handle_errors(futures):
    failed = []
    for future in as_completed(futures):
        config = futures[future]
        try:
            result = future.result()
            print(result)
        except Exception as e:
            failed.append(e)

    if failed:
        print("THESE FAILED WITH THESE ERROR MESSAGES:")
        for fail in failed:
            print(fail)
        raise ValueError(f"{len(failed)} error(s) ocurred while processing")
    else:
        print("NO FAILURES DURING PROCESSING")


# In[23]:


config_file_name = "config.json"
#config_file_name = "config_fail.json"
config_path = f"{path_prefix_file_api}/{config_file_name}"
config_list = read_notebook_config(config_path)

# run processing in parallel and handle errors
with ThreadPoolExecutor(max_workers=2) as executor:
    futures = {executor.submit(write_csv_file_to_lh_table_parallel, config): config for config in config_list}
handle_errors(futures)

