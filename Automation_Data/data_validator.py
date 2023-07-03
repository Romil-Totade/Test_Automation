


import os
import yaml as yml
import great_expectations as ge
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
import pandas as pd
from google.cloud import storage
from datetime import datetime
import numpy as np

class DataValidator:
    def __init__(self,  expected_result):
        """
        Initializes the DataValidator class with the necessary parameters.

        """
        self.expected_result = expected_result      
    
    def connection(self, project_name, dataset_name, loc):
        yaml = YAMLHandler()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= loc
        self.context = ge.get_context()
        datasource_config = {
            "name": "my_bigquery_datasource",
            "class_name": "Datasource",
            "execution_engine": {
                "class_name": "SqlAlchemyExecutionEngine",
                "connection_string": "bigquery://snappy-way-381212/Output_Table",
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
        
        con_str = "bigquery://{project_name}/{dataset_name}".format(project_name=project_name,dataset_name=dataset_name)
        datasource_config['execution_engine']['connection_string'] = con_str  
            
        self.context.test_yaml_config(yaml.dump(datasource_config))    
        self.table_list = self.context.get_available_data_asset_names()['my_bigquery_datasource']['default_inferred_data_connector_name']  
        
        print('Displaying Relevant table:')
        for i in self.table_list:
            if i.startswith('egr_test'):
                print(i)
        self.context.add_datasource(**datasource_config)
        
        
    def Data_read(self):

        self.Ex_data = pd.read_csv(self.expected_result)
        display(self.Ex_data)
        
    def Data_preprocessor(self, data):
        data = data.fillna(0)
        data = data.replace('null',0)
        data['prd_pln_id'] = data['prd_pln_id'].astype(int)
        data['exp_fctry_date'] = pd.to_datetime(data['exp_fctry_date'])

        
        data = data[['prd_pln_id','fg_do_num','do_fg_ship_num','bu_cd','do_sts_cd','exe_pln_sts_cd',
'dely_core_rspnsblty','wh_cd','exp_fctry_date','eta_wh','prcs_trgt_etd_wks',
'prcs_trgt_exp_fctry_date_acqstn_rang','wh_alctn_date','init_alctn_date',
'nw_itm_trgt_srch_flg','cont_itm_alctn_date','max_trnspt_lt',
'fstst_trnspt_mthd_cd','fstst_eta_wh','opnty_loss_amt','gp_ttl_amt',
'crnt_prc_ttl_amt','trnspt_mthd_alctn_trgt_flg','trnspt_mthd_asgned_flg','prty','rgst_dtime']]
        
        return data
        
        
    def get_data(self,table_name):
    
        batch_request = BatchRequest(
        datasource_name="my_bigquery_datasource",
        data_connector_name="default_inferred_data_connector_name",
        data_asset_name=table_name,
        batch_spec_passthrough={"create_temp_table": False},
        )

        self.context.add_or_update_expectation_suite(expectation_suite_name="test_suite")
        validator = self.context.get_validator(
            batch_request=batch_request, expectation_suite_name="test_suite")

        data1 = validator.head(fetch_all=True)
        return data1
    
    def case1(self,prd_pln_id,fg_do_num, do_fg_ship_num,actual ):
        temp_data = actual[actual['prd_pln_id'] == prd_pln_id] 
        if temp_data.shape[0] == 0:
            ver,index = self.case2(False,fg_do_num, do_fg_ship_num,temp_data)
            return False,0
        elif temp_data.shape[0] == 1:
            temp,index = self.case2(True,fg_do_num, do_fg_ship_num, temp_data )
            return temp,index
        else:
            temp,index= self.case2(True,fg_do_num, do_fg_ship_num, temp_data)
            return temp, index

    def case2(self,case1_verdict,fg_do_num, do_fg_ship_num,actual):
        temp_data = actual[(actual['fg_do_num'] == fg_do_num) & (actual['do_fg_ship_num'] == do_fg_ship_num)] 
        if temp_data.shape[0] == 0:
            if case1_verdict == False:
                print('No Match Found with either columns')
                pass
                return False,0
            else:
                pass
                print('Match found with prd_pln_id column only')
                return False,0
        elif temp_data.shape[0] > 0:
            if case1_verdict == False:
                pass
                print('Matched with case 2 but failed case 1')
                return False,0
            else:
                print('Matched both the cases')
                index = temp_data.index.values[0]
                return True,index

    def final_data_creation(self, actual, expected, index,ver):
        match_data = []
        final_data = []
     
        for i in self.col_list:
            actual = actual.filter(items=[index], axis=0)
            if ver:
                if actual[i].values[0] == expected[i].values[0]:
                    final_data.append(actual[i].values[0])
                    match_data.append('Match')
                    print ('column {} matched' .format(i))
                else:
                    print ('column {} not matched' .format(i))
                    match_data.append('Mismatch')
                    final_data.append(actual[i].values[0])
            else :
                if actual[i].values[0] == expected[i].values[0]:
                    print ('column {} matched' .format(i))
                else:
                    print ('column {} not matched' .format(i))
        return final_data, match_data

    def compute(self):
        
        self.final_data1 = []
        self.match_data1 = []

        self.Ex_data = self.Data_preprocessor(self.Ex_data)
        self.col_list = list(self.Ex_data.columns)
        for table_name in self.table_list:
            if table_name.startswith('egr_'):

                self.ac_data = self.get_data(table_name)
                self.ac_data = self.Data_preprocessor(self.ac_data)
                
                for iter_row in range(len(self.Ex_data)):

                    self.Ex_data = self.Data_preprocessor(self.Ex_data)
                    ver,index  = self.case1(self.Ex_data.iloc[iter_row]['prd_pln_id'], self.Ex_data.iloc[iter_row]['fg_do_num'],self.Ex_data.iloc[iter_row]['do_fg_ship_num'], self.ac_data)
                    if ver:
                        self.final_bq_table_name = table_name.split('.')[1]
                        print('Table - ',table_name)
                        final_temp, match_temp = self.final_data_creation(self.ac_data, self.Ex_data, index,ver)
                        self.final_data1.append(final_temp)
                        self.match_data1.append(match_temp)
                        
                    else:

                        print('Table - ',table_name)
                        dump_data1, dump_data2= self.final_data_creation(self.ac_data, self.Ex_data, index,ver)
                    print('==========================')
                    
        final_dataframe = pd.DataFrame(self.final_data1)
        match_dataframe = pd.DataFrame(self.match_data1)
        
        if match_dataframe.empty:
            self.dataframe_transposed = self.Ex_data.transpose()
            self.dataframe_transposed.reset_index(inplace=True, drop=True)
            self.dataframe_transposed.columns = ['Expected']
            self.dataframe_transposed['Columns'] = self.col_list
            self.dataframe_transposed['Actual'] = '-'
            self.dataframe_transposed['Validation'] = 'Mismatch'
            self.dataframe_transposed = self.dataframe_transposed[['Columns', 'Expected', 'Actual', 'Validation']]
        else:
            final_dataframe.columns = self.col_list
            match_dataframe.columns = self.col_list
            final_result_v1 =  self.Ex_data.append(final_dataframe)
            final_result_v2 =  final_result_v1.append(match_dataframe)
            final_result_v2 = final_result_v2[self.col_list]
            self.dataframe_transposed = final_result_v2.transpose() 
            self.dataframe_transposed.columns = ['Expected', 'Actual','Validation']
            self.dataframe_transposed[self.final_bq_table_name] = self.dataframe_transposed.index
            self.dataframe_transposed.reset_index(inplace=True, drop=True)
            self.dataframe_transposed = self.dataframe_transposed[[self.final_bq_table_name,'Expected', 'Actual', 'Validation']]

        pd.set_option('display.max_rows', 60)
        display(self.dataframe_transposed)
    
    def upload_to_gcs(self, key, bucket_name, loc, filename):
        
        self.dataframe_transposed.to_csv(filename, index=False)
        client = storage.Client.from_service_account_json(json_credentials_path=key)
        bucket = client.get_bucket(bucket_name)
        object_name_in_gcs_bucket = bucket.blob(loc)
        object_name_in_gcs_bucket.upload_from_filename(filename)
        print('Successfully Uploaded')
