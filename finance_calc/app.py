import json
import pandas as pd
from functions import get_direct_formulae, calculate_values, input_data_recalc, calculate_values_str_formula, get_metrics_mapping, to_camel_case, get_single_fs_values, get_cogs_finance_mapping, get_variable_taxes
import os
import requests
from fetchS3 import get_combined_files
import copy
import growthMoves
from datetime import datetime, timedelta
from statusService import sendProductFetchingStatus

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
    
    # data['midMonthData'] = True

    parsed_data = copy.deepcopy(data)
    print('input data', data)
    
    try:

        if 'midMonthData' in data: 
            midMonthData = data['midMonthData']
        else:
            midMonthData = False
        if 'generateMoves' in data: 
            generateMoves = data['generateMoves']
        else:
            generateMoves = False        

        print('midMonthData',midMonthData)
        print('generateMoves',generateMoves)

        # INPUT DATA RECALCULATION
        api_link = data['apiLink']
        metric_mapping_data = data['metricMappingData']
        last12CYMonthsArr = data['last12CYMonthsArr']
        input_data_mapping = data['inputDataMapping']
        try:
            tax_applicable = data['taxApplicable']
        except Exception as e:
            tax_applicable = 'NOT_APPLICABLE'
            print(f'taxApplicable not present in input payload, Error: {e}')
        print('tax_applicable',tax_applicable)
        if midMonthData:
            input_data_mid_mapping = data['inputDataMidMonthMapping']
        finance_statement_table = data['financeStatementTable']
        company_id = data['companyId']
        job_id = data['jobId']
        product_cost = data['productCost']
        
        isFirstTime = data['isFirstTime'] if data['isFirstTime'] != None else False
        print('isFirstTime', isFirstTime)
        
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

        if tax_applicable == 'VARIABLE':
            taxes_finance_mapping = get_variable_taxes(product_cost)
        else:
            taxes_finance_mapping = {}

        ## CALCULATING FINANCIAL STATEMENTS
        financial_statement_values = []
        fst_temp_values = {}
        for fst_metric in finance_statement_table:
            all_calc_value, fst_temp_values = get_single_fs_values(fst_metric, last12CYMonthsArr, input_data_mapping, metrics_mapping, company_id, unit, fst_temp_values, calculated_input_data, required_calc_metrics_names, finance_statement_table, required_calc_metrics, cogs_finance_mapping, tax_applicable, taxes_finance_mapping, 'Monthly')
    #         print(all_calc_value)
            financial_statement_values += all_calc_value

        moves_result = []

        generateMoves = True
        if generateMoves:
            mid_calculated_input_data = []
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
                    if 'sales_mid_month' in entry:
                        entry['sales'] = entry.pop('sales_mid_month')
                    if 'salesMidMonth' in entry:
                        entry['sales'] = entry.pop('salesMidMonth')
                    return entry   

                # mid_calculated_input_data = []

                mid_required_calc_metrics = []
                mid_required_calc_metrics_names = []
                for i in metric_mapping_data:
                    if i['formula'] != '':
                        mid_required_calc_metrics.append(i)
                        mid_required_calc_metrics_names.append(i['fsmName'])

                mid_metrics_mapping = get_metrics_mapping(metric_mapping_data)

                mid_product_cost = [process_entry(entry) for entry in product_cost]    
            #     return mid_product_cost
                mid_cogs_finance_mapping, mid_updated_product_costs = get_cogs_finance_mapping(mid_product_cost)

                if tax_applicable == 'VARIABLE':
                    mid_taxes_finance_mapping = get_variable_taxes(mid_product_cost)
                else:
                    mid_taxes_finance_mapping = {}

                last13CYMonthsArr = last12CYMonthsArr
                current_month = datetime.now().strftime('%m-%Y')
                last13CYMonthsArr.insert(0, current_month)

                ## CALCULATING FINANCIAL STATEMENTS
                mid_financial_statement_values = []
                mid_fst_temp_values = {}
                for mid_fst_metric in finance_statement_table:
                    mid_all_calc_value, mid_fst_temp_values = get_single_fs_values(mid_fst_metric, last13CYMonthsArr, input_data_mid_mapping, mid_metrics_mapping, company_id, unit, mid_fst_temp_values, mid_calculated_input_data, mid_required_calc_metrics_names, finance_statement_table, mid_required_calc_metrics, mid_cogs_finance_mapping, tax_applicable, mid_taxes_finance_mapping, 'Mid Month')
                    mid_financial_statement_values += mid_all_calc_value    

                #Divide value by to inplaces where cost is scheduled for the month and need to be calculated for midmonth
                try:
                    for item in mid_calculated_input_data:
                        if item.get('isScheduled', False):
                            try:
                                item['updated_cost'] = item['updated_cost'] / 2
                            except Exception as e:
                                # If updated_cost is NaN or inf, set to 0 (or any other invalid value)
                                item['updated_cost'] = 0 
                                print(f'Error in dividing(item) mid month scheduled values: {e}')
                except Exception as e:
                    print(f'Error in dividing mid month scheduled values: {e}')   
                         
            # print('financial_statement_values',financial_statement_values)
            # print('parsed_data',parsed_data)
            # print('mid_financial_statement_values',mid_financial_statement_values)
            moves_result = growthMoves.calculate_growth(financial_statement_values, parsed_data, mid_financial_statement_values)

            # print('moves_result',moves_result)

    #     return moves_result,"",""
    #     return financial_statement_values,parsed_data,mid_financial_statement_values,calculated_input_data,mid_calculated_input_data
            body = {
                    "midFinanceStatementValues": mid_financial_statement_values,
                    "financeStatementMoves": moves_result,
                    "updatedProductCosts": updated_product_costs,
                    "inputData": calculated_input_data + mid_calculated_input_data,
                    "financeStatementValues": financial_statement_values,
                    "companyId": company_id,
                    "jobId": job_id,
                    "isFirstTime": isFirstTime
                }
        
        else:
            body = {
                    "midFinanceStatementValues": [],
                    "financeStatementMoves": [],
                    "updatedProductCosts": updated_product_costs,
                    "inputData": calculated_input_data,
                    "financeStatementValues": financial_statement_values,
                    "companyId": company_id,
                    "jobId": job_id,
                    "isFirstTime": isFirstTime
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
    except Exception as e:
        print(f'Error in finance calc {e}')
        isProductUpdate = data['isProductUpdate'] if data['isProductUpdate'] is not None else False
        if(isProductUpdate):
            payload = {
                    "jobId": data['jobId'],
                    "dfStatusId": None,
                    "from": "FINANCE_CALC",
                    "companyId": data['companyId']
                }
            sendProductFetchingStatus(data['apiLink'], payload)

# lambda_handler({
#     "bucket":"uploadfiles-jigsaw",
#     "key":"97_dev/financeCalcDev-FinanceCalc-tUyY8ekJ6gGl/payload-1712673962366.json"
# }, None)
