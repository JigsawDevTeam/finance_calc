import json
import pandas as pd
from functions import get_direct_formulae, calculate_values, input_data_recalc, calculate_values_str_formula, get_metrics_mapping, get_financial_statement_values

def lambda_handler(event, context):
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
     
    return {
        "statusCode": 200,
        "body": {
            "calculated_input_data": calculated_input_data,
            "financial_statement_values": financial_statement_values,
        },
    }
