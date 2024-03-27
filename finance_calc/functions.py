import pandas as pd
import numpy as np

def get_direct_formulae():
    return {
        'Net Sales': 'Sales-GST Collected+Other Income',
        'Gross Profit': 'Net Sales-Cost of Goods Sold',
        'Gross Profit %': '(Gross Profit/Sales)*100',
        'CM1': 'Gross Profit-Shipping Cost-Packaging Cost',
        'CM1 %': '(CM1/Net Sales)*100',
        'CM2': 'CM1-Ad Spend-Platform Commission-Additional Cost',
        'CM2 %': '(CM2/Net Sales)*100',
    }

def calculate_values(formula, value_map):
    try:
        # Split the formula by '+' to separate the numbers
        numbers = formula.split('+')
        updated_values = []

        for key in numbers:
            try:
                if key in value_map:
                    in_effect_string = value_map[key]['inEffectValue']
                    in_effect_string = to_camel_case(in_effect_string)
                    updated_values.append(str(value_map[key][in_effect_string]))
                else:
                    updated_values.append('0')
            except ValueError as e:
                print(e)
                updated_values.append('0')

        # Join the updated values with '+' to reconstruct the formula
        try:
            updated_formula = '+'.join(updated_values)
            return eval(updated_formula)
        except:
            print('ff', updated_values, updated_formula)
        return 0
        
    except Exception as e:
        print(f'Error in calculate values: {e} {formula}')
        return 0

def input_data_recalc(required_calc_metrics, last12CYMonthsArr, input_data_mapping, company_id, unit, calculated_input_data, date):
    try:
        dummy = {
            'fs_metric_id': 0,
            'company_id': company_id,
            'updated_cost': 0,
            'api_cost': 0,
            'unit': unit,
            'month_year': '',
            'in_effect_value': 'updated_cost',
            'is_percentage': 0,
            'percentage_value': 0,
            'relation_metric_id': 0,
            'relation_type': 'metric',
        }
        for i in required_calc_metrics:
            formula_string = i['formula']
            values = {}
            # for date in last12CYMonthsArr:
            if date in input_data_mapping:
                calculated_value = calculate_values(formula_string, input_data_mapping[date])
                values[date] = calculated_value
                newObj = {
                    **dummy,
                    'fs_metric_id': i['fsmId'],
                    'updated_cost': calculated_value,
                    'unit': unit,
                    'month_year': date,
                }
                input_data_mapping[date][i['fsmId']] = calculated_value

            calculated_input_data.append(newObj)
    except Exception as e:
        print(f'Error in input data recalculation: {e}')

def calculate_values_str_formula(formula, value_map):
    try:
        sorted_value_map = dict(sorted(value_map.items(), key=lambda item: len(item[0]), reverse=True))
        formula_string = formula
        for key, value in sorted_value_map.items():
            formula_string = formula_string.replace(str(key), str(value))
        try:
            final_value = eval(formula_string)
        except Exception as e:
            print(f'Error in calculate_values_str_formula eval: {e}')
            final_value = 0
        final_value = round(final_value, 1)
        if final_value == 0:
            return 0
        return final_value
    except Exception as err:
        print(f'ERROR IN CALCULATION calculate_values_str_formula: {err}')
        
def get_metrics_mapping(metric_mapping_data):
    metric_mapping = {}
    # Iterate through metric_mapping_data
    for current_value in metric_mapping_data:
        fs_name = current_value.get('fsName')
        if fs_name:
            if fs_name in metric_mapping:
                metric_mapping[fs_name].append(current_value)
            else:
                metric_mapping[fs_name] = [current_value]
    return metric_mapping


def to_camel_case(text):
    s = text.replace("-", " ").replace("_", " ")
    s = s.split()
    if len(text) == 0:
        return text
    return s[0] + ''.join(i.capitalize() for i in s[1:])

def getCOGS(product_cost_df):
    product_cost_df['COGS'] = np.where(product_cost_df['inEffectValue'] == 'api_cost', product_cost_df['apiCost'], product_cost_df['updatedCost']) * product_cost_df['quantity']
    unit = product_cost_df['unit'].unique()[0]
    month_wise_product_cost_df = product_cost_df.groupby('monthYear').agg({'COGS': 'sum'}).reset_index()
    month_wise_product_cost_df['COGS'] = round(month_wise_product_cost_df['COGS'], 0)

    cogs_finance_mapping = {}
    for index, row in month_wise_product_cost_df.iterrows():
        cogs_finance_mapping[row['monthYear']] = row['COGS']
    return cogs_finance_mapping

def get_cogs_finance_mapping(product_cost):
    product_cost_df = pd.DataFrame(product_cost)
    # Product Cost Recalculation
    def recalc_product_costs(product_cost_df):
        try:
            product_cost_df['percentageValue'] = product_cost_df['percentageValue'].astype(float)
            product_cost_df['price'] = product_cost_df['price'].astype(float)
            if 'isNew' in product_cost_df:
                product_cost_df['isNew'] = product_cost_df['isNew'].astype(bool).fillna(False)
            else:
                product_cost_df['isNew'] = False
            product_cost_df['recalc'] = product_cost_df['isNew'] | product_cost_df['isPercentage']
            product_cost_df['updatedCost'] = np.where((product_cost_df['isPercentage'] == True), round(((product_cost_df['price'] * product_cost_df['percentageValue']) / 100),2), product_cost_df['updatedCost'])
            recalc_product_cost_df = product_cost_df[product_cost_df['recalc'] == True].reset_index(drop = True)
            recalc_product_cost_df = recalc_product_cost_df.fillna(0)
            updated_product_costs = recalc_product_cost_df.to_dict(orient='records')
            return updated_product_costs
        except Exception as e:
            print(f'Error in recalc_product_costs: {e}')
    
    updated_product_costs = recalc_product_costs(product_cost_df)
    
    cogs_finance_mapping = getCOGS(product_cost_df)
    return cogs_finance_mapping, updated_product_costs

def get_single_fs_values(fst_metric, last12CYMonthsArr, input_data_mapping, metrics_mapping, company_id, unit, fst_temp_values, calculated_input_data, required_calc_metrics_names, finance_statement_table, required_calc_metrics, cogs_finance_mapping,moveType):
    direct_formulae = get_direct_formulae()
    allData = []
    if moveType == 'Monthly':
        mid_month_boolean_value = 0
    else:
        mid_month_boolean_value = 1  
    # CHECK IF LOGIC IS ON LAMBDA
    if not fst_metric['isLambdaLogic']:
        tempObj = {}
        for date in last12CYMonthsArr:
            total = 0

            # CHECK IF THIS IS COST OF GOODS SOLD
            if fst_metric['name'] == 'Cost of Goods Sold':
                if date in cogs_finance_mapping:
                    total = cogs_finance_mapping[date]
                    # Storing for DB storage
                    dummy = {
                        'financial_statement_id': fst_metric['id'],
                        'company_id': company_id,
                        'month_year': date,
                        'value': total,
                        'unit': unit,
                        'is_mid_month': mid_month_boolean_value
                    }
                    allData.append(dummy)
            else:
                # NORMAL METRICS
                if date in input_data_mapping:
                    for metrics in metrics_mapping[fst_metric['name']]:
                        metricId = str(metrics['fsmId'])
                        
                        if (date in input_data_mapping) and (metricId in input_data_mapping[date]):
                            ## RECALCULATE THE INPUT METRICS FIRST
                            ## CODE FOR RECALC
                            if metrics['fsmName'] in required_calc_metrics_names:
                                input_data_recalc(required_calc_metrics, last12CYMonthsArr, input_data_mapping, company_id, unit, calculated_input_data, date)
                            
                            
                            ## RECALCULTED VALUES WILL BE DIRECTLY CHANGED TO INPUT_DATA_MAPPING as array and object are passed by reference
                            calculate_relation_input_data(input_data_mapping, date, fst_temp_values, finance_statement_table, metricId, calculated_input_data, mid_month_boolean_value)

                            inEffectString = input_data_mapping[date][metricId]['inEffectValue']
                            inEffectString = to_camel_case(inEffectString)
                            # print(input_data_mapping[date][metricId][inEffectString], inEffectString)
                            total += input_data_mapping[date][metricId][inEffectString] if input_data_mapping[date][metricId][inEffectString] != None else 0

                    # Storing for DB storage
                    dummy = {
                        'financial_statement_id': fst_metric['id'],
                        'company_id': company_id,
                        'month_year': date,
                        'value': total,
                        'unit': unit,
                        'is_mid_month': mid_month_boolean_value
                    }
                    allData.append(dummy)

            # Storing for data requirement in lambda logic FST
            if date in fst_temp_values:
                fst_temp_values[date][fst_metric['name']] = total
            else:
                fst_temp_values[date] = {fst_metric['name']: total}

    else:
    ## LAMBDA LOGIC METRICS
        total = 0
        if fst_metric['name'] in direct_formulae:
            # DIRECT FORMULA METRICS CALCULATION
            for date in last12CYMonthsArr:
                    if date in fst_temp_values:
                        formula = direct_formulae[fst_metric['name']]
                        total = calculate_values_str_formula(formula, fst_temp_values[date])
                        # print(fst_metric['name'], total, date)

                        # Storing for data requirement in lambda logic FST
                        fst_temp_values[date][fst_metric['name']] = total

                        # Storing for DB storage
                        dummy = {
                            'financial_statement_id': fst_metric['id'],
                            'company_id': company_id,
                            'month_year': date,
                            'value': total,
                            'unit': unit,
                            'is_mid_month': mid_month_boolean_value
                        }
                        allData.append(dummy)
        else:
            # GROWTH % CALCULATION
            growth = {}
            for i in range(len(last12CYMonthsArr) - 1, 0, -1):
                prevDate = last12CYMonthsArr[i]
                currDate = last12CYMonthsArr[i-1]
                curr = fst_temp_values[currDate]['Net Sales']
                prev = fst_temp_values[prevDate]['Net Sales']

                if prev == 0:
                    diff = 100
                else:
                    diff = round(((curr - prev)/prev)*100,1)

                growth[currDate] = diff

                # Storing for data requirement in lambda logic FST
                fst_temp_values[currDate][fst_metric['name']] = diff

                # Storing for DB storage
                dummy = {
                    'financial_statement_id': fst_metric['id'],
                    'company_id': company_id,
                    'month_year': currDate,
                    'value': diff,
                    'unit': unit,
                    'is_mid_month': mid_month_boolean_value
                }
                allData.append(dummy)
                print(allData[len(allData) - 1])
                
    return allData, fst_temp_values

def calculate_relation_input_data(input_data_mapping, date, fst_temp_values, finance_statement_table, metricId, calculated_input_data, mid_month_boolean_value = 0):
    try:
        single_input_data = input_data_mapping[date][metricId]
        relation_type = single_input_data['relationType']
        relation_metric_id = single_input_data['relationMetricId']
        if not relation_metric_id:
            return

        temp_value = -1
        if relation_type == 'statement':
            for i in finance_statement_table:
                if relation_metric_id == i['id']:
                    metric_value = fst_temp_values[date][i['name']]
                    temp_value = metric_value
        elif relation_type == 'metric':
            input_mapping_data = input_data_mapping[date][str(relation_metric_id)]
            temp_value = input_mapping_data[to_camel_case(input_mapping_data['inEffectValue'])]


        if temp_value != -1:

            if single_input_data['isPercentage']:
                recalc_value = round(((single_input_data['percentageValue'] * temp_value ) / 100), 2)
                single_input_data['updatedCost'] = recalc_value    

            dummy = {
                'fs_metric_id': single_input_data['metricId'],
                'company_id': single_input_data['companyId'],
                'updated_cost': single_input_data['updatedCost'],
                'api_cost': single_input_data['apiCost'],
                'unit': single_input_data['unit'],
                'month_year': single_input_data['monthYear'],
                'in_effect_value': single_input_data['inEffectValue'],
                'is_percentage': single_input_data['isPercentage'],
                'percentage_value': single_input_data['percentageValue'],
                'relation_metric_id': single_input_data['relationMetricId'],
                'relation_type': single_input_data['relationType'],
                'is_mid_month': mid_month_boolean_value,
            }

            if 'editedBy' in single_input_data and 'editedTime' in single_input_data:
                dummy = {
                    **dummy,
                    'edited_by': single_input_data['editedBy'],
                    'edited_time': single_input_data['editedTime']
                }

            calculated_input_data.append(dummy)
    except Exception as e:
        print('metricId', metricId)
        print(f'Error in calculate_relation_input_data: {e}')