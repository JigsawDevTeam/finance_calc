import json
import pandas as pd
from functions import get_direct_formulae, calculate_values, input_data_recalc, calculate_values_str_formula, get_metrics_mapping, to_camel_case, get_single_fs_values, get_cogs_finance_mapping
import os
import requests
from fetchS3 import get_combined_files
import copy
import growthMoves
from datetime import datetime, timedelta

def lambda_handler(event, context):
    environment = os.environ.get('ENVIRONMENT')
    print('ENVIROMENT', environment)
    # print('event', event)

    envConfigObj = event
    # Retrieve the bucket name and key from the event
    key = envConfigObj['key']

    # Download the payload from S3
    data = get_combined_files().get_json_payload(key)
    
    try:        
        data = json.loads(data)
    except:
        data = data
    
    data['midMonthData'] = True

    parsed_data = copy.deepcopy(data)
    # print('data', data)

    if 'midMonthData' in data: 
        midMonthData = data['midMonthData']
    else:
        midMonthData = False
    if 'generateMoves' in data: 
        generateMoves = data['generateMoves']
    else:
        generateMoves = False        
    
    # INPUT DATA RECALCULATION
    api_link = data['apiLink']
    metric_mapping_data = data['metricMappingData']
    last12CYMonthsArr = data['last12CYMonthsArr']
    input_data_mapping = data['inputDataMapping']
    if midMonthData:
        input_data_mid_mapping = data['inputDataMidMonthMapping']
    finance_statement_table = data['financeStatementTable']
    company_id = data['companyId']
    job_id = data['jobId']
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
    cogs_finance_mapping, updated_product_costs = get_cogs_finance_mapping(product_cost)
    
    ## CALCULATING FINANCIAL STATEMENTS
    financial_statement_values = []
    fst_temp_values = {}
    for fst_metric in finance_statement_table:
        all_calc_value, fst_temp_values = get_single_fs_values(fst_metric, last12CYMonthsArr, input_data_mapping, metrics_mapping, company_id, unit, fst_temp_values, calculated_input_data, required_calc_metrics_names, finance_statement_table, required_calc_metrics, cogs_finance_mapping, 'Monthly')
        financial_statement_values += all_calc_value
        
    moves_result = []
    
    if generateMoves:
        mid_financial_statement_values = []
        if midMonthData:
            def process_entry(entry):
        #         Drop the 'quantity' key
                if 'quantity' in entry:
                    del entry['quantity']
        #         Rename 'quantity' key to 'quantityMidMonth'
                if 'quantity_mid_month' in entry:
                    entry['quantity'] = entry.pop('quantity_mid_month')
                if 'quantityMidMonth' in entry:
                    entry['quantity'] = entry.pop('quantityMidMonth')
                return entry   

            mid_calculated_input_data = []

            mid_required_calc_metrics = []
            mid_required_calc_metrics_names = []
            for i in metric_mapping_data:
                if i['formula'] != '':
                    required_calc_metrics.append(i)
                    required_calc_metrics_names.append(i['fsmName'])

            mid_metrics_mapping = get_metrics_mapping(metric_mapping_data)

            mid_product_cost = [process_entry(entry) for entry in product_cost]    
        #     return mid_product_cost
            mid_cogs_finance_mapping, mid_updated_product_costs = get_cogs_finance_mapping(mid_product_cost)

            last13CYMonthsArr = last12CYMonthsArr
            current_month = datetime.now().strftime('%m-%Y')
            last13CYMonthsArr.insert(0, current_month)

            ## CALCULATING FINANCIAL STATEMENTS
            mid_financial_statement_values = []
            mid_fst_temp_values = {}
            for mid_fst_metric in finance_statement_table:
                mid_all_calc_value, mid_fst_temp_values = get_single_fs_values(mid_fst_metric, last13CYMonthsArr, input_data_mid_mapping, mid_metrics_mapping, company_id, unit, mid_fst_temp_values, mid_calculated_input_data, mid_required_calc_metrics_names, finance_statement_table, mid_required_calc_metrics, mid_cogs_finance_mapping,'Mid Month')
                mid_financial_statement_values += mid_all_calc_value    

        moves_result = growthMoves.calculate_growth(financial_statement_values, parsed_data, mid_financial_statement_values)

#     return moves_result,"",""
#     return financial_statement_values,parsed_data,mid_financial_statement_values
        body = {
                "midFinanceStatementValues": mid_financial_statement_values,
                "financeStatementMoves": moves_result,
                "updatedProductCosts": updated_product_costs,
                "inputData": calculated_input_data,
                "financeStatementValues": financial_statement_values,
                "companyId": company_id,
                "jobId": job_id
            }
    
    else:
        body = {
                "midFinanceStatementValues": [],
                "financeStatementMoves": [],
                "updatedProductCosts": updated_product_costs,
                "inputData": calculated_input_data,
                "financeStatementValues": financial_statement_values,
                "companyId": company_id,
                "jobId": job_id
            }
    print(body)
    # return body,'',''
    headers = {
            'Content-Type': 'application/json'
        }
    
    print(f'Sending response to {api_link}')

    
    
    # print('body', body)
    requests.post(
            f"{api_link}/Finance/insertFinanceValuesToDB", data=json.dumps(body), headers=headers)
     
    return {
        "statusCode": 200,
        "body": body,
    }

# lambda_handler({
#     "bucket":"uploadfiles-jigsaw",
#     "key":"26_dev/financeCalcDev-FinanceCalc-tUyY8ekJ6gGl/payload-1710326660751.json"
# }, None)
