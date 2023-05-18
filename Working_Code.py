#!/usr/bin/env python
# coding: utf-8

# In[1]:


from data_validator import DataValidator


# In[2]:


from google.oauth2 import service_account
import json
import os
from datetime import datetime


# In[3]:


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/shreyashindurkar28/snappy-way-381212-b30f7ab5ac58.json"
credentials = service_account.Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])


# In[4]:


# Define your parameters
project_id = "snappy-way-381212"
dataset_name = "Final_Automation"
table_name = "trial_data"
bucket_name = "trial-shin"
config_file = "Data_Configuration.yaml"
credentials = credentials


# In[5]:


# Create an instance of the Data_Validator class
validator = DataValidator(project_id, dataset_name, table_name, bucket_name, config_file, credentials)


# In[6]:


# Call the `code()` method to set up the BigQuery data source configuration in Great Expectations
validator.setup_bigquery_datasource()


# In[7]:


# Call the `compute()` method to perform data validation
result = validator.compute()


# In[8]:


# Print the validation results
for table, validation_result in result.items():
    if validation_result:
        print(f"Validation passed for table: {table}")
    else:
        print(f"Validation failed for table: {table}")


# In[9]:


json_results = json.dumps(result)
json_results


# In[10]:


# Upload the JSON results to GCS with a unique file name
file_name = f"GE_8_validation_results_18-05{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
file_name


# In[11]:


# Uploading Results into GCP bucket
validator.upload_to_gcs(file_name, json_results)


# In[ ]:




