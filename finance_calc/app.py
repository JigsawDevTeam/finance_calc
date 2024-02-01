import json
import pandas as pd
from functions import get_direct_formulae, calculate_values, input_data_recalc, calculate_values_str_formula, get_metrics_mapping, to_camel_case, get_single_fs_values 
import os
import requests
from fetchS3 import get_combined_files

def lambda_handler(event, context):
    environment = os.environ.get('ENVIRONMENT')
    print('ENVIROMENT', environment)
    print('event', event)

    envConfig = event
    try:
        envConfigObj = json.loads(envConfig)
    except:
        envConfigObj = envConfig

    # Retrieve the bucket name and key from the event
    key = envConfigObj['key']

    # Download the payload from S3
    data = get_combined_files().get_json_payload(key)

    try:        
        data = json.loads(data)
    except:
        data = data
    
    # INPUT DATA RECALCULATION
    api_link = data['apiLink']
    metric_mapping_data = data['metricMappingData']
    last12CYMonthsArr = data['last12CYMonthsArr']
    input_data_mapping = data['inputDataMapping']
    finance_statement_table = data['financeStatementTable']
    company_id = data['companyId']
    product_cost = data['productCost']
    unit = '₹'

    calculated_input_data = []

    required_calc_metrics = []
    required_calc_metrics_names = []
    for i in metric_mapping_data:
        if i['formula'] != '':
            required_calc_metrics.append(i)
            required_calc_metrics_names.append(i['fsmName'])
    
    metrics_mapping = get_metrics_mapping(metric_mapping_data)
    
    ## CALCULATING FINANCIAL STATEMENTS
    financial_statement_values = []
    fst_temp_values = {}
    updated_product_costs = []
    for fst_metric in finance_statement_table:
        all_calc_value, fst_temp_values, updated_product_costs = get_single_fs_values(fst_metric, last12CYMonthsArr, input_data_mapping, metrics_mapping, company_id, unit, fst_temp_values, calculated_input_data, required_calc_metrics_names, finance_statement_table, required_calc_metrics, product_cost, updated_product_costs)
        financial_statement_values += all_calc_value
        
    body = {
            "updatedProductCosts": updated_product_costs,
            "inputData": calculated_input_data,
            "financeStatementValues": financial_statement_values,
            "companyId": company_id
        }
    
    headers = {
            'Content-Type': 'application/json'
        }
    
    print(f'Sending response to {api_link}')
    requests.post(
            f"{api_link}/Finance/insertFinanceValuesToDB", data=json.dumps(body), headers=headers)
     
    return {
        "statusCode": 200,
        "body": body,
    }

# with open('events/event.json', 'r') as file:
#     data = json.load(file)

# print(lambda_handler(data, {}))