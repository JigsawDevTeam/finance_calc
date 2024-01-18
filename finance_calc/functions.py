def get_direct_formulae():
    return {
        'Net Sales': 'Sales-Returns+GST+Other Income',
        'Gross Profit': 'Net Sales-Cost of Goods Sold',
        'Gross Profit %': 'Sales/Gross Profit',
        'CM1': 'Gross Profit-Shipping Cost-Packaging',
        'CM2': 'CM1-Ad Spend-Platform Comission-Additional Costs',
    }

def calculate_values(formula, value_map):
    try:
        # Split the formula by '+' to separate the numbers
        numbers = formula.split('+')
        updated_values = []

        for key in numbers:
            try:
                if key in value_map:
                    updated_values.append(str(value_map[key]))
                else:
                    updated_values.append('0')
            except ValueError as e:
                print(e)
                updated_values.append('0')

        # Join the updated values with '+' to reconstruct the formula
        updated_formula = '+'.join(updated_values)

        return eval(updated_formula)
    except Exception as e:
        print(f'Error in calculate values: {e}')
        return 0

def input_data_recalc(required_calc_metrics, last12CYMonthsArr, input_data_mapping, company_id, unit):
    calculated_input_data = []
    try:
        dummy = {
            'metricId': 0,
            'companyId': company_id,
            'updatedCost': 0,
            'apiCost': 0,
            'unit': unit,
            'monthYear': '',
            'inEffectValue': 'apiCost'
        }
        for i in required_calc_metrics:
            formula_string = i['formula']
            values = {}
            for date in last12CYMonthsArr:
                if date in input_data_mapping:
                    calculated_value = calculate_values(formula_string, input_data_mapping[date])
                    values[date] = calculated_value
                    newObj = {
                        **dummy,
                        'metricId': i['fsmId'],
                        'apiCost': calculated_value,
                        'unit': unit,
                        'monthYear': date,
                    }
                    input_data_mapping[date][i['fsmId']] = calculated_value

                calculated_input_data.append(newObj)
    except Exception as e:
        print(f'Error in input data recalculation: {e}')
    return calculated_input_data

def calculate_values_str_formula(formula, value_map):
    try:
        sorted_value_map = dict(sorted(value_map.items(), key=lambda item: len(item[0]), reverse=True))
        formula_string = formula
        for key, value in sorted_value_map.items():
            formula_string = formula_string.replace(str(key), str(value))
        final_value = eval(formula_string)
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

def get_financial_statement_values(finance_statement_table, last12CYMonthsArr, cogs_finance_mapping, input_data_mapping, metrics_mapping, company_id, unit):
    financial_statement_values = []
    fst_temp_values = {}
    direct_formulae = get_direct_formulae()
    for fst_metric in finance_statement_table:

        # CHECK IF LOGIC IS ON LAMBDA
        if not fst_metric['isLambdaLogic']:
            tempObj = {}
            for date in last12CYMonthsArr:
                total = 0

                # CHECK IF THIS IS COST OF GOODS SOLD
                if fst_metric['name'] == 'Cost of Goods Sold':
                    total = cogs_finance_mapping[date]
                else:
                    # NORMAL METRICS
                    if date in input_data_mapping:
                        for metrics in metrics_mapping[fst_metric['name']]:
                            metricId = str(metrics['fsmId'])
                            if (date in input_data_mapping) and (metricId in input_data_mapping[date]):
                                total += input_data_mapping[date][metricId]

                        # Storing for DB storage
                        dummy = {
                            'financialStatementId': fst_metric['id'],
                            'companyId': company_id,
                            'monthYear': date,
                            'value': total,
                            'unit': unit,
                        }
                        financial_statement_values.append(dummy) 

                # Storing for data requirement in lambda logic FST
                if date in fst_temp_values:
                    fst_temp_values[date][fst_metric['name']] = total
                else:
                    fst_temp_values[date] = {fst_metric['name']: total}
        else:
        ## LAMBDA LOGIC METRICS
            if fst_metric['name'] in direct_formulae:
                # DIRECT FORMULA METRICS CALCULATION
                total = 0
                for date in last12CYMonthsArr:
                        if date in fst_temp_values:
                            formula = direct_formulae[fst_metric['name']]
                            total = calculate_values_str_formula(formula, fst_temp_values[date])

                            # Storing for data requirement in lambda logic FST
                            fst_temp_values[date][fst_metric['name']] = total

                            # Storing for DB storage
                            dummy = {
                                'financialStatementId': fst_metric['id'],
                                'companyId': company_id,
                                'monthYear': date,
                                'value': total,
                                'unit': unit,
                            }
                            financial_statement_values.append(dummy) 
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
                    fst_temp_values[currDate][fst_metric['name']] = total

                    # Storing for DB storage
                    dummy = {
                        'financialStatementId': fst_metric['id'],
                        'companyId': company_id,
                        'monthYear': currDate,
                        'value': total,
                        'unit': unit,
                    }
                    financial_statement_values.append(dummy)
    return financial_statement_values