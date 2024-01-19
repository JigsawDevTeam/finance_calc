import json
import pandas as pd
from functions import get_direct_formulae, calculate_values, input_data_recalc, calculate_values_str_formula, get_metrics_mapping, get_financial_statement_values
import os
import requests

def lambda_handler(event, context):
    environment = os.environ.get('ENVIRONMENT')
    api_link = os.environ.get('JIGSAWAPILINK')
    print('ENVIROMENT', environment)
    data = event
    
    # INPUT DATA RECALCULATION
    metric_mapping_data = data['metricMappingData']
    last12CYMonthsArr = data['last12CYMonthsArr']
    cogs_finance_mapping = data['cogsFinanceMapping']
    input_data_mapping = data['inputDataMapping']
    finance_statement_table = data['financeStatementTable']
    companyId = data['companyId']
    unit = '₹'

    requiredCalcMetrics = []
    for i in metric_mapping_data:
        if i['formula'] != '':
            requiredCalcMetrics.append(i)
            
    ## RECALCULATED INPUT DATA FIELDS
    calculated_input_data = input_data_recalc(requiredCalcMetrics, last12CYMonthsArr, input_data_mapping, companyId, unit)
    
    
    metrics_mapping = get_metrics_mapping(metric_mapping_data)
    
    ## CALCULATING FINANCIAL STATEMENTS
    financial_statement_values = get_financial_statement_values(finance_statement_table, last12CYMonthsArr, cogs_finance_mapping, input_data_mapping, metrics_mapping, companyId, unit)

    body = {
            "inputData": calculated_input_data,
            "financeStatementValues": financial_statement_values,
            "companyId": companyId
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