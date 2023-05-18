#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import yaml
import great_expectations as ge
import numpy as np
from google.cloud import storage
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler

class DataValidator:
    def __init__(self, project_id, dataset_name, table_name, 
                 bucket_name, config_file,credentials):
        """
         Initializes the Octopus class with the necessary parameters.
        :param project_id: the ID of the Google BigQuery project
        :param dataset_name: the name of the BigQuery dataset
        :param table_name: the name of the BigQuery table
        :param bucket_name: the name of the Google Cloud Storage bucket where data is stored
        :param config_file: the path to the YAML configuration
        file containing validation expectations
        """
        self.project_id = project_id
        self.dataset_name = dataset_name
        self.table_name = table_name
        self.bucket_name = bucket_name
        self.config_file = config_file
        self.credentials=credentials

    def setup_bigquery_datasource(self):
        """This Function sets up a BigQuery
         data source configuration in Great Expectations,
         tests the configuration,
         retrieves the available  table names,
         and adds the data source to the context.
        """
        yaml = YAMLHandler()
        self.context = ge.get_context()
        datasource_config = {
            "name": "my_bigquery_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": f"bigquery://{self.project_id}/{self.dataset_name}",
            },
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["default_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "include_schema_name": True,
                },
            },
        }

        con_str = "bigquery://{project_name}/{dataset_name}"
        con_str = con_str.format(project_name= self.project_id,dataset_name=self.dataset_name)
        datasource_config['execution_engine']['connection_string'] = con_str
        self.context.test_yaml_config(yaml.dump(datasource_config))
        self.table_list = self.context.get_available_data_asset_names()['my_bigquery_datasource']['default_inferred_data_connector_name']
        self.context.add_datasource(**datasource_config)

    def read_config(self):
        """
        Reads and returns the contents
        of the configuration file
        using the yaml library.
        """
        with open(self.config_file, "r") as f:
            config = yaml.safe_load(f)
        return config

    def validator(self, my_dict, df_ge, j):
        """
        Validates the data in a given
        dictionary (my_dict) against a set
        of expectations using the great_expectations library.
        :param my_dict: a dictionary containing the
        column names, types, and
        max lengths for a table
        :param df_ge: a Great Expectations dataframe object
        representing the data to be validated
        :return: a tuple consisting of a boolean
        (True if validation passed, False if not)
        and a dictionary containing the
                 validation results
        """
        final = {}
        required, maxlen, datatype = {}, {}, {}

        # Validating each column
        for i in range(len(my_dict[j]['column_names'])):
            required[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_not_be_null(
                column=my_dict[j]['column_names'][i]
            )['success']

            if my_dict[j]['column_types'][i] != 'date':
                if (
                    df_ge.expect_column_values_to_not_be_null
                    (column=my_dict[j]['column_names'][i])['success']
                    == False
                ):
                    if my_dict[j]['column_types'][i] == 'int64':
                        datatype[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_be_of_type(
                            column=my_dict[j]['column_names'][i], type_='float64'
                        )['success']
                    else:
                        datatype[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_be_of_type(
                            column=my_dict[j]['column_names'][i], type_=my_dict[j]['column_types'][i]
                        )['success']
                else:
                    datatype[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_be_of_type(
                        column=my_dict[j]['column_names'][i], type_=my_dict[j]['column_types'][i]
                    )['success']

            if my_dict[j]['column_types'][i] == 'date':
                datatype[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_match_strftime_format(
                    column=my_dict[j]['column_names'][i], strftime_format=my_dict[j]['max_lengths'][i]
                )['success']

            if my_dict[j]['column_types'][i] in ['int64', 'float']:
                maxlen[my_dict[j]['column_names'][i]] = df_ge.expect_column_values_to_be_between(
                    column=my_dict[j]['column_names'][i], min_value=1, max_value=my_dict[j]['max_lengths'][i]
                )['success']

            if my_dict[j]['column_types'][i] == 'str':
                maxlen[my_dict[j]['column_names'][i]] = df_ge.expect_column_value_lengths_to_be_between(
                    column=my_dict[j]['column_names'][i], min_value=1, max_value=my_dict[j]['max_lengths'][i]
                )['success']

        final['Required'] = required
        final['DataType'] = datatype
        final['Maxlen'] = maxlen

        if (
            (False in list(final['Required'].values()))
            or (False in list(final['DataType'].values()))
            or (False in list(final['Maxlen'].values()))
            ) :
            return False, final
        else:
            return True, final

    def get_data(self, table_name):
        """
        Retrieves data from a specific table
        in the BigQuery data source,
        applies expectations defined
        in the "test_suite" expectation suite,
        and returns the data that
        satisfies those expectations.
        """
        batch_request = BatchRequest(
            datasource_name="my_bigquery_datasource",
            data_connector_name="default_inferred_data_connector_name",
            data_asset_name=table_name,
            batch_spec_passthrough={"create_temp_table": False},
        )

        self.context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
        validator = self.context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite"
        )

        data1 = validator.head(fetch_all=True)
        return data1
    def compute(self):
        """
        Performs data validation on each table
        in the dataset by querying each table
        and converting the resulting data into a
        Pandas dataframe.The method then calls the validator()
        method to validate the data against
        the expectations in the configuration file.
        If the validation passes, the method prints
        "Validation passed for table: {i}",
        where "i" is the name of the table.
        If validation fails, the method prints
        "Validation failed for table: {i}" and
        prints the validation results.
        """
        final_result = {}

        for t in self.table_list:
            df = self.get_data(t)
            i = t.split('.')[1]
            date_cols = [
                self.read_config()[i]['column_names'][g]
                for g in range(len(self.read_config()[i]['column_names']))
                if self.read_config()[i]['column_types'][g] == 'date'
            ]

            for k in date_cols:
                df[k] = df[k].astype(str)
            df = df.replace('None', np.nan)
            df_ge = ge.from_pandas(df)
            final_result[i], tt = self.validator(self.read_config(), df_ge, i)

            if final_result[i] == False:
                for j in tt['Required']:
                    if tt['Required'][j] == False:
                        print(f'{i} ------ There exists Null values in {j} column.')
                for j in tt['DataType']:
                    if tt['DataType'][j] == False:
                        print(f'{i} ------ The Datatype of {j} column does not match as described in config file.')
                for j in tt['Maxlen']:
                    if tt['Maxlen'][j] == False:
                        print(f'{i} ------ The Maxlen of {j} column does not match as described in config file.')
            elif final_result[i] == True:
                print(f'{i} ------ Validation Successful')
        return final_result

    def upload_to_gcs(self,file_name, content):
        """
        Uploads a file to the specified
        Google Cloud Storage bucket.
        :param file_name: The name of the file
        to be uploaded
        :param content: The content of the file
        to be uploaded
        """
        storage_client = storage.Client(project=self.project_id, credentials=self.credentials)
        bucket = storage_client.get_bucket(self.bucket_name)
        blob = bucket.blob(file_name)
        blob.upload_from_string(content)

