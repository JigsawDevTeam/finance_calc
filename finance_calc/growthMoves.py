import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
import copy

def flatten_data(data, path=''):
    flattened = []
    for month_year, entries in data.items():
        for key, values in entries.items():
            flat_record = {"monthYear": month_year, **values}
            flattened.append(flat_record)
    return flattened

def valuegetter(df,col_name,col_value,field):
    value = df[df[col_name] == col_value][field].iloc[0]
    return value

def getPerChange(prev, curr):
    if prev == 0:
        return 0
    return ((curr - prev) / prev ) * 100

def custom_round(number):
    """
    Rounds a number to 1 decimal place if the value after the decimal is not 0.
    Otherwise, rounds to 0 decimal places.
    """
    rounded_number = round(number, 1)
    if rounded_number % 1 == 0:
        return round(rounded_number)  # Rounds to 0 decimal places if there's no decimal part
    else:
        return rounded_number
    
def update_fsm_name(row):
    if row['fsName'] == 'Sales' and 'Sales' not in row['fsmName']:
        return f"{row['fsmName']} Sales"
    else:
        return row['fsmName']    
    
def format_percentage(n):
    """
    Function to format a percentage such that it returns up to 1 decimal if the value
    after the decimal is not .0; otherwise, it returns the whole number.
    """
    try:
        n = round(n,1)
        if n % 1 == 0:
            return int(n)
        else:
            return round(n, 1)
    except:
        return n
    
def extract_label(text):
    """
    Function to extract the label from a given text in the format:
    'Platform [Label]' and return it as 'Platform's Label'.
    """
    try:
        if '[' in text and ']' in text:
            platform, label = text.split('[')
            label = label.strip(']')
            return f"{platform.strip()}'s {label}"
        else:
            return text
    except:
        return text

def format_value(number):
    """
    Correctly formats a number based on its size with specified precision, handling edge cases:
    - Numbers below 100 are returned as integers without any decimal part.
    - Numbers between 100 and 99,999 are expressed in 'K' (thousands) with up to 1 decimal place if needed.
      For numbers very close to 100,000, they should be treated specially to round up to '1L'.
    - Numbers 100,000 or more are expressed in 'L' (lakhs), with precision to 1 decimal place where relevant.
      Decimal part is omitted if it results in a 0 after rounding.
    """
    if number < 100:
        return str(int(number))
    elif 100 <= number < 99950:
        k_value = round(number / 1000, 1)
        return f"{k_value:.0f}K" if k_value.is_integer() else f"{k_value}K"
    elif number >= 99949:
        if number < 100000:  # Special handling for numbers close to 100,000
            return "1L"
        else:
            l_value = round(number / 100000, 1)
            return f"{l_value:.0f}L" if l_value.is_integer() else f"{l_value}L"

def affected_metrics(primary_dict, standard_change, keys_to_consider):
    try:
        # Filter the dictionary by the specified keys
        filtered_data_dict = {k: v for k, v in primary_dict.items() if k in keys_to_consider}
        
        if standard_change > 0:
            filtered_dict = {k: v for k, v in filtered_data_dict.items() if v > 0}
        elif standard_change < 0:
            filtered_dict = {k: v for k, v in filtered_data_dict.items() if v < 0}
        else:
            filtered_dict = {}
            
        if len(filtered_dict) > 0:
            # Sort the filtered dictionary by the absolute values in descending order
            sorted_data = sorted(filtered_dict.items(), key=lambda item: abs(item[1]), reverse=True)

            if len(sorted_data) > 1:# and abs(abs(sorted_data[0][1]) - abs(sorted_data[1][1])) / abs(sorted_data[0][1]) < 0.10:
                result_dict = f"{sorted_data[0][0]} and {sorted_data[1][0]}"  # Keep top 2 elements based on their original values
            else:
                result_dict = sorted_data[0][0]  # Keep only the top element based on its original value
        else:
            result_dict = ''

        return result_dict
    except:
        return ''
        
def select_top_elements_abs(data_dict,metric_rate_change):
    """
    Selects the top 1 or 2 elements from a dictionary based on their absolute values.
    Filters out elements with non-positive absolute values, sorts the remaining elements in descending order
    of their absolute values, and selects the top elements based on a 15% difference criterion.
    
    Parameters:
    - data_dict: A dictionary with elements as keys and numerical values.
    
    Returns:
    - A dictionary with the selected top element(s) based on absolute values.
    """
    
    if metric_rate_change > 0:
        # Filter out items with non-positive absolute values
        filtered_data_dict = {k: v for k, v in data_dict.items() if v > 0}
    elif metric_rate_change < 0:
        filtered_data_dict = {k: v for k, v in data_dict.items() if v < 0}
    else:
        filtered_data_dict = {}
    

    # Proceed only if the filtered dictionary is not empty
    if len(filtered_data_dict) > 0:
        # Sort the filtered dictionary by the absolute values in descending order
        sorted_data = sorted(filtered_data_dict.items(), key=lambda item: abs(item[1]), reverse=True)

        # Check if the top two elements have less than 15% difference in their absolute values
        if len(sorted_data) > 1 and abs(abs(sorted_data[0][1]) - abs(sorted_data[1][1])) / abs(sorted_data[0][1]) < 0.15:
            result_dict = dict(sorted_data[:2])  # Keep top 2 elements based on their original values
        else:
            result_dict = dict([sorted_data[0]])  # Keep only the top element based on its original value
    else:
        result_dict = {}
    
    return result_dict

def adjust_decimal_corrected(number):
    try:
        if number == np.inf or number == -np.inf or np.isnan(number):
            number = 0

        # Since value will never be negative, we start by trying to round to 1 decimal place
        rounded_value = round(number, 1)
        if rounded_value > 0:
            return rounded_value
        else:
            # Try rounding to 2 decimal places if 1 decimal place gives 0
            rounded_value = round(number, 2)
            if rounded_value > 0:
                return rounded_value
            else:
                # Try rounding to 3 decimal places if 2 decimal places still gives 0
                rounded_value = round(number, 3)
                if rounded_value > 0:
                    return rounded_value
                else:
                    # If value is still 0 after rounding to 3 decimal places, return 0
                    return 0
    except:
        return number

def sales_spend_move(cm2_change_pct,cm2_last,cm2_this,sales_change_pct,sales_last,sales_this,spend_change_pct,spend_last,spend_this,last_month_name,metrics_affected,threshold,moveType='Monthly'):
    sales_change = sales_this - sales_last
    spend_change = spend_this - spend_last
    
    if cm2_this > cm2_last:
        primary_effect = "increased"  # Use 'increased' if primary metric fell
    else:
        primary_effect = "reduced"  # Use 'reduced' if primary metric increased
    
    s_s_ratio_this = (spend_this / sales_this)*100
    s_s_ratio_last = (spend_last / sales_last)*100
    
    s_s_ratio_this = format_percentage(s_s_ratio_this)
    s_s_ratio_last = format_percentage(s_s_ratio_last)
    
    # Determine trends
    cm2_trend = "increased" if cm2_change_pct > 0 else "fell"
    sales_trend = "increased" if sales_change_pct > threshold else "decreased" if sales_change_pct < -(threshold) else "steady"
    spend_trend = "increased" if spend_change_pct > threshold else "decreased" if spend_change_pct < -(threshold) else "steady"
    
    formatted_sales_change = f"{format_percentage(sales_change_pct)}"
    formatted_spend_change = f"{format_percentage(spend_change_pct)}"
    
    if sales_trend != spend_trend:
    
        if moveType == 'Monthly':
            message = f"CM2 % in {last_month_name} {cm2_trend} to {format_percentage(cm2_this)}% from {format_percentage(cm2_last)}%."
            summary = f"CM2 % in {last_month_name} {cm2_trend} to <b>{format_percentage(cm2_this)}%</b> from <b>{format_percentage(cm2_last)}%</b>"
            suffix = ''
        else:
            message = f"CM2 % this month {cm2_trend} to {format_percentage(cm2_this)}% from {format_percentage(cm2_last)}% last month."
            summary = f"CM2 % this month {cm2_trend} to <b>{format_percentage(cm2_this)}%</b> from <b>{format_percentage(cm2_last)}%</b> last month"
            suffix = ' at this time last month'

        # Conditions and messages
        if cm2_trend == "increased":
            if sales_trend == "increased" and spend_trend == "decreased":
                # Sales Increase & Spend Decrease
                if s_s_ratio_this > 0:
                    message += f"<br>This is because your Ad Spend to Net Sales ratio decreased to {s_s_ratio_this}%."
            elif sales_change > spend_change and spend_trend == "increased":
                # Sales Increase > Spend Increase
                if s_s_ratio_this > 0:
                    message += f"<br>This is because your Ad Spend to Net Sales ratio decreased to {s_s_ratio_this}%, despite a {formatted_spend_change}% increase in Ad Spend."
            elif sales_change < spend_change and spend_trend == "decreased":
                # Sales decrease < Spend decrease
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio decreased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}."
            elif sales_trend == "increased" and spend_trend == "steady":
                # Sales Increase & Steady Spend
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio decreased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}, despite no increase in Ad Spend."
            elif sales_trend == "steady" and spend_trend == "decreased":
                # Steady Sales & Spend Decrease
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio decreased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}, despite no decrease in Net Sales."
            else:
                message = ''
                
        elif cm2_trend == "fell":
            if sales_trend == "decreased" and spend_trend == "increased":
                # Sales Decrease & Spend Increase
                if s_s_ratio_this > 0:
                    message += f"<br>This is because your Ad Spend to Net Sales ratio increased to {s_s_ratio_this}%."
            elif sales_change < spend_change and spend_trend == "decreased":
                # Sales decrease > Spend decrease
                if s_s_ratio_this > 0:
                    message += f"<br>This is because your Ad Spend to Net Sales ratio increased to {s_s_ratio_this}%, despite a {formatted_spend_change}% decrease in Ad Spend."
            elif sales_change < spend_change and spend_trend == "increased":
                # Sales increase < Spend increase
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio increased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}."
            elif sales_trend == "steady" and spend_trend == "increased":
                # Sales Steady & Spend Increase
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio increased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}, despite no increase in Net Sales."
            elif sales_trend == "decreased" and spend_trend == "steady":
                # Sales Decrease & Steady Spend
                if (s_s_ratio_this > 0 and s_s_ratio_last > 0) and (s_s_ratio_this != s_s_ratio_last):
                    message += f"<br>This is because your Ad Spend to Net Sales ratio increased to {s_s_ratio_this}% from {s_s_ratio_last}%{suffix}, despite no decrease in Ad Spend."
            else:
                message = ''
            
        if message != '' and metrics_affected != '':
            message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
                
        return message,summary 
    else:
        return '',''

def cogs_sales_move(gp_change_pct,gp_last,gp_this,sales_change_pct,sales_last,sales_this,cogs_change_pct,cogs_last,cogs_this,last_month_name,metrics_affected,threshold,moveType='Monthly'):
    # Determine trends
    gp_trend = "increased" if gp_change_pct > 0 else "fell"
    sales_trend = "increased" if sales_change_pct > threshold else "decreased" if sales_change_pct < -(threshold) else "steady"
    cogs_trend = "increased" if cogs_change_pct > threshold else "decreased" if cogs_change_pct < -(threshold) else "steady"

    if gp_this > gp_last:
        primary_effect = "increased"  # Use 'increased' if primary metric fell
    else:
        primary_effect = "reduced"  # Use 'reduced' if primary metric increased
    
    try:
        formatted_sales_change = f"{abs(sales_change_pct):.1f}" if not (sales_change_pct).is_integer() else f"{abs(sales_change_pct):.0f}"
    except:
        formatted_sales_change = f"{abs(sales_change_pct):.1f}"
    try:
        formatted_cogs_change = f"{abs(cogs_change_pct):.1f}" if not (cogs_change_pct).is_integer() else f"{abs(cogs_change_pct):.0f}"
    except:
        formatted_cogs_change = f"{abs(cogs_change_pct):.1f}"
    
#     print('formatted_sales_change',formatted_sales_change)
#     print('formatted_cogs_change',formatted_cogs_change)
    
    message = ''
    summary = ''
    
    if moveType == 'Monthly':
        if abs(cogs_change_pct) >= abs(sales_change_pct): 
            summary = f"Gross Profit % in {last_month_name} {gp_trend} to <b>{format_percentage(gp_this)}%</b> from <b>{format_percentage(gp_last)}%</b>"
            message = f"Gross Profit % in {last_month_name} {gp_trend} to {format_percentage(gp_this)}% from {format_percentage(gp_last)}%."
            if gp_trend == "fell":
                if sales_trend == "decreased" and cogs_trend == "increased":
                    # Sales Decrease & COGS Increase
                    message += f"<br>This could be because of higher sales of low margin products"
                elif sales_trend == "steady" and cogs_trend == "increased":
                    # Sales Steady & COGS Increase
                    message += f"<br>This could be because of higher sales of low margin products"
                else:
                    message = ''
            elif gp_trend == "increased":
                if sales_trend == "increased" and cogs_trend == "decreased":
                    # Sales Increase & COGS Decrease
                    message += f"<br>This could be because of higher sales of high margin products"
                elif sales_trend == "steady" and cogs_trend == "decreased":
                    # Steady Sales & COGS Decrease
                    message += f"<br>This could be because of higher sales of high margin products"
                else:
                    message = ''
                    
    else:
        if abs(cogs_change_pct) >= abs(sales_change_pct): 
            summary = f"Gross Profit % this month {gp_trend} to <b>{format_percentage(gp_this)}%</b> from <b>{format_percentage(gp_last)}%</b> last month"
            message = f"Gross Profit % this month {gp_trend} to {format_percentage(gp_this)}% from {format_percentage(gp_last)}% last month."
            if gp_trend == "fell":
                if sales_trend == "decreased" and cogs_trend == "increased":
                    # Sales Decrease & COGS Increase
                    message += f"<br>This could be because of higher sales of low margin products."
                elif sales_trend == "steady" and cogs_trend == "increased":
                    # Sales Steady & COGS Increase
                    message += f"<br>This could be because of higher sales of low margin products."
                else:
                    message = ''
            elif gp_trend == "increased":
                if sales_trend == "increased" and cogs_trend == "decreased":
                    # Sales Increase & COGS Decrease
                    message += f"<br>This could be because of higher sales of high margin products."
                elif sales_trend == "steady" and cogs_trend == "decreased":
                    # Steady Sales & COGS Decrease
                    message += f"<br>This could be because of higher sales of high margin products."
                else:
                    message = ''
            
    if message != '' and metrics_affected != '':
        message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
                
    return message,summary

def primary_secondary_single_move(primary_metric, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, metrics_affected, last_month_name, moveType='Monthly',secondary_metrics=True):
    # Determine the direction of change for the primary metric
    if primary_this > primary_last:
        primary_change = "increased"
        primary_effect = "increased"  # Use 'increased' if primary metric fell
    else:
        primary_change = "fell"
        primary_effect = "reduced"  # Use 'reduced' if primary metric increased

    if moveType != 'Monthly':
        primary_change_pct = custom_round(getPerChange(primary_last, primary_this))    
        
    # Determine the direction of change for the secondary metric
    if secondary_this > secondary_last:
        secondary_change = "an increase"
    else:
        secondary_change = "a decrease"

    if secondary_metric == 'D2C Sales':    
        secondary_metric = 'Website Sales'
        
    secondary_metric = extract_label(secondary_metric)
    
    if primary_metric == 'Monthly Growth':
        if (primary_change == "increased" and secondary_change == "an increase") or (primary_change == "fell" and secondary_change == "a decrease"):
            pass
        else:
            secondary_metrics = False
        if secondary_metrics:
            if moveType == 'Monthly':
                # Construct the message
                summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
                message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." \
                          f"<br>This is due to {secondary_change} in growth of {secondary_metric} to {format_percentage(secondary_this)}% from {format_percentage(secondary_last)}%." 

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."

            else:
                summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
                message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." \
                          f"<br>This is due to {secondary_change} in growth of {secondary_metric} to {format_percentage(secondary_this)}% from {format_percentage(secondary_last)}% at this time last month." 

                if metrics_affected != '':
                        message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
        else:
            if moveType == 'Monthly':
                # Construct the message
                summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
                message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%."

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."

            else:
                summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
                message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month."
                if metrics_affected != '':
                        message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
    
    else:
        if moveType == 'Monthly':
            # Construct the message
            summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
            message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." \
                      f"<br>This is due to {secondary_change} in {secondary_metric} to {format_value(secondary_this)} from {format_value(secondary_last)}." 

            if metrics_affected != '':
                message += f"<br>This also {primary_effect} the {metrics_affected} for the month."

        else:
            summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
            message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." \
                      f"<br>This is due to {secondary_change} in {secondary_metric} to {format_value(secondary_this)} from {format_value(secondary_last)} at this time last month." 

            if metrics_affected != '':
                message += f"<br>This also {primary_effect} the {metrics_affected} for the month."

    return message,summary

def primary_secondary_double_move(primary_metric, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, metrics_affected, last_month_name, moveType = 'Monthly', secondary_metrics=True):
    # Determine the direction of change for the primary metric
    if primary_this > primary_last:
        primary_change = "increased"
        primary_effect = "increased"  # Use 'increased' if primary metric fell
    else:
        primary_change = "fell"
        primary_effect = "reduced"  # Use 'reduced' if primary metric increased
    
    if moveType != 'Monthly':
        primary_change_pct = custom_round(getPerChange(primary_last, primary_this))
    
    # Determine the direction of change for the first secondary metric
    if secondary_this_1 > secondary_last_1:
        secondary_change_1 = "an increase"
    else:
        secondary_change_1 = "a decrease"
    
    # Determine the direction of change for the second secondary metric
    if secondary_this_2 > secondary_last_2:
        secondary_change_2 = "an increase"
    else:
        secondary_change_2 = "a decrease"
    
    if secondary_metric_1 == 'D2C Sales':    
        secondary_metric_1 = 'Website Sales'
    if secondary_metric_2 == 'D2C Sales':    
        secondary_metric_2 = 'Website Sales'
    
    secondary_metric_1 = extract_label(secondary_metric_1)
    secondary_metric_2 = extract_label(secondary_metric_2)
    
    if primary_metric == 'Monthly Growth':
        if (primary_change == "increased" and secondary_change_1 == "an increase" and secondary_change_2 == "an increase") or (primary_change == "fell" and secondary_change_1 == "a decrease" and secondary_change_2 == "a decrease"):
            pass
        else:
            secondary_metrics = False
        
        if secondary_metrics:
            if moveType == 'Monthly':
                # Construct the message
                summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
                message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." \
                          f"<br>This is due to {secondary_change_1} in growth of {secondary_metric_1} to {format_percentage(secondary_this_1)}% from {format_percentage(secondary_last_1)}% " \
                          f"or {secondary_change_2} in growth of {secondary_metric_2} to {format_percentage(secondary_this_2)}% from {format_percentage(secondary_last_2)}%." 

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."  
            else:
                summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
                message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." \
                          f"<br>This is due to {secondary_change_1} in growth of {secondary_metric_1} to {format_percentage(secondary_this_1)}% from {format_percentage(secondary_last_1)}% at this time last month " \
                          f"or {secondary_change_2} in growth of {secondary_metric_2} to {format_percentage(secondary_this_2)}% from {format_percentage(secondary_last_2)}% at this time last month." 

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
        else:
            if moveType == 'Monthly':
                # Construct the message
                summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
                message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%."

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."  
            else:
                summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
                message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month."

                if metrics_affected != '':
                    message += f"<br>This also {primary_effect} the {metrics_affected} for the month."
    
    else:
        if moveType == 'Monthly':
            # Construct the message
            summary = f"{primary_metric} in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
            message = f"{primary_metric} in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." \
                      f"<br>This is due to {secondary_change_1} in {secondary_metric_1} to {format_value(secondary_this_1)} from {format_value(secondary_last_1)} " \
                      f"or {secondary_change_2} in {secondary_metric_2} to {format_value(secondary_this_2)} from {format_value(secondary_last_2)}." 

            if metrics_affected != '':
                message += f"<br>This also {primary_effect} the {metrics_affected} for the month."  
        else:
            summary = f"{primary_metric} this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
            message = f"{primary_metric} this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." \
                      f"<br>This is due to {secondary_change_1} in {secondary_metric_1} to {format_value(secondary_this_1)} from {format_value(secondary_last_1)} at this time last month " \
                      f"or {secondary_change_2} in {secondary_metric_2} to {format_value(secondary_this_2)} from {format_value(secondary_last_2)} at this time last month." 

            if metrics_affected != '':
                message += f"<br>This also {primary_effect} the {metrics_affected} for the month."

    
    return message,summary


def getTagline(metric: str, change: str):
    '''Returns the tagline for moves

    @param metric: The metric for which the tagline is to be returned
    @param change: The direction of change of the metric

    @return: The tagline for the metric
    '''

    try:
        tagline_store = {
            'Monthly Growth %': {
                'increase': "Monthly Growth % Up",
                'decrease': "Monthly Growth % Dropped"
            },
            'Gross Profit %': {
                'increase': "Gross Profit % Up",
                'decrease': "Gross Profit % Dropped"
            },
            'CM1 %': {
                'increase': "CM1 % Up",
                'decrease': "CM1 % Dropped"
            },
            'CM2 %': {
                'increase': "CM2 % Up",
                'decrease': "CM2 % Dropped"
            },
            'EBITDA Margin': {
                'increase': "EBITDA Margin Up",
                'decrease': "EBITDA Margin Dropped"
            },
            'Net Cash Margin': {
                'increase': "Net Cash Margin Up",
                'decrease': "Net Cash Margin Dropped"
            }
        }
        return tagline_store[metric][change]
    except Exception as e:
        print(f'Error in getTagline: {e}')
        return ''

    
def insightDict(metric: str, insight, summary, change, midMonthValue):
    growth_type = "MOVES_TRENDING_UP" if change == 'increase' else "MOVES_TRENDING_DOWN"

    return {
      "insight": insight,
      "suggestion": "",
      "summary": summary,  
      "type": growth_type,
      "tagline": getTagline(metric, change),
      "is_mid_month": midMonthValue
    }

def growth_subset_df(financial_statement_id_mapping,inner_this,inner_last,inner_before_last,metric_name1,metric_name2,metric_rate_change=0):
    if metric_name1 != '':
        start_idx = financial_statement_id_mapping.index[financial_statement_id_mapping['name'] == metric_name1].tolist()[0]
    end_idx = financial_statement_id_mapping.index[financial_statement_id_mapping['name'] == metric_name2].tolist()[0]

    if metric_name1 == '':
        df_subset = financial_statement_id_mapping.loc[0:end_idx-1]
    else:
        df_subset = financial_statement_id_mapping.loc[start_idx+1:end_idx-1]

    ids = df_subset['financial_statement_id'].to_list()
    
    metric_this_df = inner_this[inner_this['fsId'].isin(ids)]
    metric_last_df = inner_last[inner_last['fsId'].isin(ids)]
    metric_before_last_df = inner_before_last[inner_before_last['fsId'].isin(ids)]

    metric_this_sec = list(metric_this_df['fsmName'].unique())
    metric_last_sec = list(metric_last_df['fsmName'].unique())
    metric_before_last_sec = list(metric_before_last_df['fsmName'].unique())
    
    common_metric_sec = list(set(metric_this_sec) & set(metric_last_sec) & set(metric_before_last_sec))
    
    # Function to calculate the growth differences
    def calculate_growth_differences(metric_this_df, metric_last_df, metric_before_last_df, common_metric_sec):
        growth_differences = {}
        for metric_name in common_metric_sec:
            this_sec_value = metric_this_df[metric_this_df['fsmName'] == metric_name]['finalValue'].values[0]
            last_sec_value = metric_last_df[metric_last_df['fsmName'] == metric_name]['finalValue'].values[0]
            before_last_sec_value = metric_before_last_df[metric_before_last_df['fsmName'] == metric_name]['finalValue'].values[0]

            growth_this_to_last = this_sec_value - last_sec_value 
            growth_last_to_before_last = last_sec_value - before_last_sec_value 

            growth_difference = growth_this_to_last - growth_last_to_before_last
            growth_differences[metric_name] = growth_difference

        return growth_differences


    # Calculate growth differences
    metric_difference_dict = calculate_growth_differences(metric_this_df, metric_last_df, metric_before_last_df, common_metric_sec)
    
    
#     return metric_difference_dict
    
#     metric_merged_df = pd.merge(metric_this_df, metric_last_df, on='fsmName', suffixes=('_this', '_last'))

#     # Calculate the difference in 'finalValue' for each element
#     metric_merged_df['difference'] = metric_merged_df['finalValue_this'] - metric_merged_df['finalValue_last']

#     # Convert the 'element' and 'difference' to a dictionary
#     metric_difference_dict = pd.Series(metric_merged_df.difference.values,index=metric_merged_df.fsmName).to_dict()
    
    filtered_dict = {k: v for k, v in metric_difference_dict.items() if k in common_metric_sec}    
    
    result = select_top_elements_abs(filtered_dict,metric_rate_change)
    
    return result


def subset_df(financial_statement_id_mapping,inner_this,inner_last,metric_name1,metric_name2,metric_rate_change=0):
    if metric_name1 != '':
        start_idx = financial_statement_id_mapping.index[financial_statement_id_mapping['name'] == metric_name1].tolist()[0]
    end_idx = financial_statement_id_mapping.index[financial_statement_id_mapping['name'] == metric_name2].tolist()[0]

    if metric_name1 == '':
        df_subset = financial_statement_id_mapping.loc[0:end_idx-1]
    else:
        df_subset = financial_statement_id_mapping.loc[start_idx+1:end_idx-1]
        
    if metric_name2 == 'CM2':
        df_subset = df_subset[df_subset['name'] != 'Ad Spend']

    ids = df_subset['financial_statement_id'].to_list()
    
    metric_this_df = inner_this[inner_this['fsId'].isin(ids)]
    metric_last_df = inner_last[inner_last['fsId'].isin(ids)]

    metric_this_sec = list(metric_this_df['fsmName'].unique())
    metric_last_sec = list(metric_last_df['fsmName'].unique())
    common_metric_sec = list(set(metric_this_sec) & set(metric_last_sec))
    
    metric_merged_df = pd.merge(metric_this_df, metric_last_df, on='fsmName', suffixes=('_this', '_last'))

    # Calculate the difference in 'finalValue' for each element
    metric_merged_df['difference'] = metric_merged_df['finalValue_this'] - metric_merged_df['finalValue_last']

    # Convert the 'element' and 'difference' to a dictionary
    metric_difference_dict = pd.Series(metric_merged_df.difference.values,index=metric_merged_df.fsmName).to_dict()
    
    filtered_dict = {k: v for k, v in metric_difference_dict.items() if k in common_metric_sec}    
    
    result = select_top_elements_abs(filtered_dict,metric_rate_change)
    
    return result

def net_cash_move(net_cash_this , net_cash_last, net_cash_change, loan_this, loan_last, sales_this , sales_last, last_month_name ,moveType='Monthly'):
    netCashMove = ''
    summary = ''
    if net_cash_change > 0:
        trend = "increased"
    else:
        trend = "fell"
       
    l_s_ratio_this = (loan_this / sales_this)*100
    l_s_ratio_last = (loan_last / sales_last)*100
    
    l_s_ratio_this = format_percentage(l_s_ratio_this)
    l_s_ratio_last = format_percentage(l_s_ratio_last)
    
    if l_s_ratio_this > l_s_ratio_last:
        secondary_change = "an increase"
    else:
        secondary_change = "a decrease"

    if moveType == 'Monthly':
        summary = f"Net Cash Margin in {last_month_name} {trend} to <b>{format_percentage(net_cash_this)}%</b> from <b>{format_percentage(net_cash_last)}%</b>."
        netCashMove = f"Net Cash Margin in {last_month_name} {trend} to {format_percentage(net_cash_this)}% from {format_percentage(net_cash_last)}%."
        if (l_s_ratio_this > 0 and l_s_ratio_last > 0) and (l_s_ratio_this != l_s_ratio_last):      
            netCashMove += f"<br>This is due to {secondary_change} in Loan Servicing to Net Sales ratio to {l_s_ratio_this}% from {l_s_ratio_last}%."
    else:
        summary = f"Net Cash Margin this month {trend} to <b>{format_percentage(net_cash_this)}%</b> from <b>{format_percentage(net_cash_last)}%</b> last month."
        netCashMove = f"Net Cash Margin this month {trend} to {format_percentage(net_cash_this)}% from {format_percentage(net_cash_last)}% last month."
        if (l_s_ratio_this > 0 and l_s_ratio_last > 0) and (l_s_ratio_this != l_s_ratio_last):
            netCashMove += f"<br>This is due to {secondary_change} in Loan Servicing to Net Sales ratio to {l_s_ratio_this}% from {l_s_ratio_last}% at this time last month."
        
    return netCashMove, summary


def ebitda_single_move(primary_this, primary_last, secondary_metric, secondary_this, secondary_last, sales_this, sales_last, last_month_name, moveType='Monthly'):
    # Determine the direction of change for the primary metric
    if primary_this > primary_last:
        primary_change = "increased"
    else:
        primary_change = "fell"

    if moveType != 'Monthly':
        primary_change_pct = custom_round(getPerChange(primary_last, primary_this))    
        
    s1_s_ratio_this = (secondary_this / sales_this)*100
    s1_s_ratio_last = (secondary_last / sales_last)*100
    
    s1_s_ratio_this = format_percentage(s1_s_ratio_this)
    s1_s_ratio_last = format_percentage(s1_s_ratio_last)
    
    # Determine the direction of change for the secondary metric
    if s1_s_ratio_this > s1_s_ratio_last:
        secondary_change = "an increase"
    else:
        secondary_change = "a decrease"
    
    if moveType == 'Monthly':
        # Construct the message
        summary = f"EBITDA Margin in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
        message = f"EBITDA Margin in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." 
        if (s1_s_ratio_this > 0 and s1_s_ratio_last > 0) and (s1_s_ratio_this != s1_s_ratio_last):
            message += f"<br>This is due to {secondary_change} in {secondary_metric} to Net Sales ratio to {s1_s_ratio_this}% from {s1_s_ratio_last}%." 

    else:
        summary = f"EBITDA Margin this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
        message = f"EBITDA Margin this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." 
        if (s1_s_ratio_this > 0 and s1_s_ratio_last > 0) and (s1_s_ratio_this != s1_s_ratio_last):
            message += f"<br>This is due to {secondary_change} in {secondary_metric} to Net Sales ratio to {s1_s_ratio_this}% from {s1_s_ratio_last}% at this time last month." 

    return message,summary

def ebitda_double_move(primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, sales_this, sales_last, last_month_name, moveType = 'Monthly'):
    # Determine the direction of change for the primary metric
    if primary_this > primary_last:
        primary_change = "increased"
    else:
        primary_change = "fell"
    
    if moveType != 'Monthly':
        primary_change_pct = custom_round(getPerChange(primary_last, primary_this))
    
    s1_s_ratio_this = ((secondary_this_1 / sales_this)*100)
    s1_s_ratio_last = ((secondary_last_1 / sales_last)*100)
    
    s1_s_ratio_this = format_percentage(s1_s_ratio_this)
    s1_s_ratio_last = format_percentage(s1_s_ratio_last)
    
    s2_s_ratio_this = ((secondary_this_2 / sales_this)*100)
    s2_s_ratio_last = ((secondary_last_2 / sales_last)*100)
    
    s2_s_ratio_this = format_percentage(s2_s_ratio_this)
    s2_s_ratio_last = format_percentage(s2_s_ratio_last)
    
    # Determine the direction of change for the first secondary metric
    if s1_s_ratio_this > s1_s_ratio_last:
        secondary_change_1 = "an increase"
    else:
        secondary_change_1 = "a decrease"
    
    # Determine the direction of change for the second secondary metric
    if s2_s_ratio_this > s2_s_ratio_last:
        secondary_change_2 = "an increase"
    else:
        secondary_change_2 = "a decrease"
    
    if moveType == 'Monthly':
        # Construct the message
        summary = f"EBITDA Margin in {last_month_name} {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b>"
        message = f"EBITDA Margin in {last_month_name} {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}%." 
        if (s1_s_ratio_this > 0 and s1_s_ratio_last > 0 and s2_s_ratio_this > 0 and s2_s_ratio_last > 0) and (s1_s_ratio_this != s1_s_ratio_last) and (s2_s_ratio_this != s2_s_ratio_last):
            message += f"<br>This is due to {secondary_change_1} in {secondary_metric_1} to Net Sales ratio to {s1_s_ratio_this}% from {s1_s_ratio_last}% " \
                       f"or {secondary_change_2} in {secondary_metric_2} to Net Sales ratio to {s2_s_ratio_this}% from {s2_s_ratio_last}%." 

    else:
        summary = f"EBITDA Margin this month {primary_change} to <b>{format_percentage(primary_this)}%</b> from <b>{format_percentage(primary_last)}%</b> last month"
        message = f"EBITDA Margin this month {primary_change} to {format_percentage(primary_this)}% from {format_percentage(primary_last)}% last month." 
        if (s1_s_ratio_this > 0 and s1_s_ratio_last > 0 and s2_s_ratio_this > 0 and s2_s_ratio_last > 0) and (s1_s_ratio_this != s1_s_ratio_last) and (s2_s_ratio_this != s2_s_ratio_last):
            message += f"<br>This is due to {secondary_change_1} in {secondary_metric_1} to Net Sales ratio to {s1_s_ratio_this}% from {s1_s_ratio_last}% at this time last month " \
                       f"or {secondary_change_2} in {secondary_metric_2} to Net Sales ratio to {s2_s_ratio_this}% from {s2_s_ratio_last}% at this time last month." 
    
    return message,summary

def calculate_growth(financial_statement_values, parsed_data, mid_financial_statement_values):
    #Thresholds
    growth_threshold = 5
    gp_threshold = 5
    cm1_threshold = 5
    cm2_threshold = 3
    ebitda_threshold = 3
    net_cash_threshold = 3
    
#     For moves testing

#     #Dates for manipulation
#     # Current Date in "mm-yyyy"
#     current_month = (datetime.now()- timedelta(days=60)).strftime('%m-%Y')

#     # Previous Month-Year
#     previous_month_year = ((datetime.now()- timedelta(days=60)).replace(day=1) - timedelta(days=1)).strftime('%m-%Y')

#     # Before Previous Month-Year
#     before_previous_month_year = ((datetime.now()- timedelta(days=60)).replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
#     before_previous_month_year = before_previous_month_year.strftime('%m-%Y')

#     two_months_before_previous_month_year = (((datetime.now()- timedelta(days=60)).replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
#     two_months_before_previous_month_year = two_months_before_previous_month_year.strftime('%m-%Y')
    
#     print("Current Month (mm-yyyy):", current_month)
#     print("Previous Month-Year (mm-yyyy):", previous_month_year)
#     print("Before Previous Month-Year (mm-yyyy):", before_previous_month_year)
#     print("Two Months Before Previous Month-Year (mm-yyyy):", two_months_before_previous_month_year)
    

    # Current Date in "mm-yyyy"
    current_month = datetime.now().strftime('%m-%Y')

    # Previous Month-Year
    previous_month_year = (datetime.now().replace(day=1) - timedelta(days=1)).strftime('%m-%Y')

    # Before Previous Month-Year
    before_previous_month_year = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    before_previous_month_year = before_previous_month_year.strftime('%m-%Y')

    # Two months before Previous Month-Year
    two_months_before_previous_month_year = ((datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    two_months_before_previous_month_year = two_months_before_previous_month_year.strftime('%m-%Y')

    print("Current Month (mm-yyyy):", current_month)
    print("Previous Month-Year (mm-yyyy):", previous_month_year)
    print("Before Previous Month-Year (mm-yyyy):", before_previous_month_year)
    print("Two Months Before Previous Month-Year (mm-yyyy):", two_months_before_previous_month_year)
    
    flattened_records = flatten_data(parsed_data['inputDataMapping'])

    df = pd.DataFrame(flattened_records)
    
    df_financial = pd.DataFrame(financial_statement_values)

    df['finalValue'] = df.apply(lambda x: x['apiCost'] if x['inEffectValue'] == 'api_cost' else x['updatedCost'], axis=1)
    df['finalValue'] = df['finalValue'].fillna(0)

    df_metric_mapping = pd.DataFrame(parsed_data['metricMappingData'])
    
    financialStatementMoves = copy.deepcopy(parsed_data['financialStatementMoves'])

    df_metric_mapping['fsId'] = df_metric_mapping['fsId'].fillna(0).astype(np.int64)
    df_metric_mapping = df_metric_mapping[['fsId','fsName','fsmId','fsmName']].rename(columns={'fsmId':'metricId'})

    inner_df = df.merge(df_metric_mapping,on='metricId',how='left')
    
    # Removing metrics which are percentage of other metrics (absolute value)
    inner_df['isPercentage'] = inner_df['isPercentage'].fillna(0)
    inner_df = inner_df[inner_df['isPercentage'] == 0].reset_index(drop=True)
    
    # Adding Sales to Sales Metrics 
    inner_df['fsmName'] = inner_df.apply(update_fsm_name, axis=1)
    
    fs_id_mapping = pd.DataFrame(parsed_data['financeStatementTable'])

    financial_statement_id_mapping = fs_id_mapping[['id', 'name']].rename(columns={'id':'financial_statement_id'})

    outer_df = df_financial.merge(financial_statement_id_mapping,on='financial_statement_id',how='left')
    
    outer_this = outer_df[outer_df['month_year'] == previous_month_year]
    outer_last = outer_df[outer_df['month_year'] == before_previous_month_year]
    
    inner_this = inner_df[inner_df['monthYear'] == previous_month_year]
    inner_last = inner_df[inner_df['monthYear'] == before_previous_month_year]
    inner_before_last = inner_df[inner_df['monthYear'] == two_months_before_previous_month_year]
    
    moves = {
            'Monthly Growth %': {},
            'Gross Profit %': {},
            'CM1 %' : {},
            'CM2 %' : {},
            'EBITDA Margin' : {},
            'Net Cash Margin' : {}
        }
    
    #Mid month data 
    if 'midMonthData' in parsed_data: 
        midMonthData = parsed_data['midMonthData']
    else:
        midMonthData = False
    if 'generateMoves' in parsed_data: 
        generateMoves = parsed_data['generateMoves']
    else:
        generateMoves = False
    
    if midMonthData:
        mid_flattened_records = flatten_data(parsed_data['inputDataMidMonthMapping'])

        mid_df = pd.DataFrame(mid_flattened_records)
        mid_df_financial = pd.DataFrame(mid_financial_statement_values)

        mid_df['finalValue'] = mid_df.apply(lambda x: x['apiCost'] if x['inEffectValue'] == 'api_cost' else x['updatedCost'], axis=1)
        mid_df['finalValue'] = mid_df['finalValue'].fillna(0)

        # Dividing finalValue by 2 where inEffectValue is 'updatedCost' to get mid month data for metrics for which full month value is updated 
        mid_df.loc[mid_df['inEffectValue'] == 'updatedCost', 'finalValue'] /= 2

        mid_inner_df = mid_df.merge(df_metric_mapping,on='metricId',how='left')

        # Removing metrics which are percentage of other metrics (absolute value)
        mid_inner_df['isPercentage'] = mid_inner_df['isPercentage'].fillna(0)
        mid_inner_df = mid_inner_df[mid_inner_df['isPercentage'] == 0].reset_index(drop=True)
        
        # Adding Sales to Sales Metrics 
        mid_inner_df['fsmName'] = mid_inner_df.apply(update_fsm_name, axis=1)

        mid_outer_df = mid_df_financial.merge(financial_statement_id_mapping,on='financial_statement_id',how='left')

        mid_outer_this = mid_outer_df[mid_outer_df['month_year'] == current_month]
        mid_outer_last = mid_outer_df[mid_outer_df['month_year'] == previous_month_year]

        mid_inner_this = mid_inner_df[mid_inner_df['monthYear'] == current_month]
        mid_inner_last = mid_inner_df[mid_inner_df['monthYear'] == previous_month_year]
        mid_inner_before_last = mid_inner_df[mid_inner_df['monthYear'] == before_previous_month_year]
        
    
    # Last month name for moves
    month_string = previous_month_year
    month, year = map(int, month_string.split('-'))
    date_object = datetime(year, month, 1)
    formatted_date = date_object.strftime("%B %Y")
    last_month_name = formatted_date
    
    try:
        try:
            gp_this = valuegetter(outer_this,'name','Gross Profit %','value') 
            gp_last = valuegetter(outer_last,'name','Gross Profit %','value')
            gp_change = getPerChange(gp_last,gp_this)
        except:
            gp_change = 0
        try:
            cm1_this = valuegetter(outer_this,'name','CM1 %','value') 
            cm1_last = valuegetter(outer_last,'name','CM1 %','value')
            cm1_change = getPerChange(cm1_last,cm1_this)
        except:
            cm1_change = 0
        try:
            cm2_this = valuegetter(outer_this,'name','CM2 %','value') 
            cm2_last = valuegetter(outer_last,'name','CM2 %','value')
            cm2_change = getPerChange(cm2_last,cm2_this)
        except:
            cm2_change = 0
        try:
            ebitda_this = valuegetter(outer_this,'name','EBITDA Margin','value') 
            ebitda_last = valuegetter(outer_last,'name','EBITDA Margin','value')
            ebitda_change = getPerChange(ebitda_last,ebitda_this)
        except:
            ebitda_change = 0
        try:
            nc_this = valuegetter(outer_this,'name','Net Cash Margin','value') 
            nc_last = valuegetter(outer_last,'name','Net Cash Margin','value')
            nc_change = getPerChange(nc_last,nc_this)
        except:
            nc_change = 0
        
        primary_change_dict = {
            "Gross Profit %" : gp_change,
            "CM1 %" : cm1_change,
            "CM2 %" : cm2_change,
            "EBITDA Margin" : ebitda_change,
            "Net Cash Margin" : nc_change
        }
    except Exception as e:
        print(f'Error in getting values(primary_change_dict) of statement:{e}')
    
    if midMonthData:
        try:
            try:
                gp_this = valuegetter(mid_outer_this,'name','Gross Profit %','value') 
                gp_last = valuegetter(outer_this,'name','Gross Profit %','value')
                gp_change = getPerChange(gp_last,gp_this)
            except:
                gp_change = 0
            try:
                cm1_this = valuegetter(mid_outer_this,'name','CM1 %','value') 
                cm1_last = valuegetter(outer_this,'name','CM1 %','value')
                cm1_change = getPerChange(cm1_last,cm1_this)
            except:
                cm1_change = 0
            try:
                cm2_this = valuegetter(mid_outer_this,'name','CM2 %','value') 
                cm2_last = valuegetter(outer_this,'name','CM2 %','value')
                cm2_change = getPerChange(cm2_last,cm2_this)
            except:
                cm2_change = 0
            try:
                ebitda_this = valuegetter(mid_outer_this,'name','EBITDA Margin','value') 
                ebitda_last = valuegetter(outer_this,'name','EBITDA Margin','value')
                ebitda_change = getPerChange(ebitda_last,ebitda_this)
            except:
                ebitda_change = 0
            try:
                nc_this = valuegetter(mid_outer_this,'name','Net Cash Margin','value') 
                nc_last = valuegetter(outer_this,'name','Net Cash Margin','value')
                nc_change = getPerChange(nc_last,nc_this)
            except:
                nc_change = 0
            
            primary_change_mid_dict = {
                "Gross Profit %" : gp_change,
                "CM1 %" : cm1_change,
                "CM2 %" : cm2_change,
                "EBITDA Margin" : ebitda_change,
                "Net Cash Margin" : nc_change
            }
        except Exception as e:
            print(f'Error in getting values(primary_change_mid_dict) of midmonth statement:{e}')
    
    try:
        if not midMonthData:
            # Monthly Growth % Move
            primary_metric = "Monthly Growth %"
            primary_metric_name = "Monthly Growth"
            primary_this = valuegetter(outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_last,'name',primary_metric,'value') 

            growth_rate_change = getPerChange(primary_last, primary_this)
            total_sales_channels = (inner_df[inner_df['fsName'] == 'Sales']).groupby('fsmName')['finalValue'].sum().reset_index()
            count_total_sales_channels = total_sales_channels[total_sales_channels['finalValue'] > 0]['fsmName'].count()

            if count_total_sales_channels > 1:
                secondary_metrics_value = True
            else:
                secondary_metrics_value = False

            mt_effected = affected_metrics(primary_change_dict, growth_rate_change, ['Gross Profit %', 'CM1 %', 'CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        
            
            if abs(growth_rate_change) >= growth_threshold:

                result_growth = growth_subset_df(financial_statement_id_mapping,inner_this,inner_last,inner_before_last,'','Monthly Growth %',growth_rate_change)
                if len(result_growth) == 1:
                    secondary_metric = list(result_growth.keys())[0]
                    secondary_this_value = valuegetter(inner_this,'fsmName',secondary_metric,'finalValue')  
                    secondary_last_value = valuegetter(inner_last,'fsmName',secondary_metric,'finalValue')  
                    secondary_before_last_value = valuegetter(inner_before_last,'fsmName',secondary_metric,'finalValue') 

                    secondary_this = getPerChange(secondary_last_value, secondary_this_value) 
                    secondary_last = getPerChange(secondary_before_last_value, secondary_last_value) 
      
                    percentage_growth_move, summary_growth_move = primary_secondary_single_move(primary_metric_name, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name,secondary_metrics=secondary_metrics_value)
        #             print(percentage_growth_move)

                if len(result_growth) == 2:
                    secondary_metric_1 = list(result_growth.keys())[0]
                    secondary_this_value_1 = valuegetter(inner_this,'fsmName',secondary_metric_1,'finalValue')  
                    secondary_last_value_1 = valuegetter(inner_last,'fsmName',secondary_metric_1,'finalValue')  
                    secondary_before_last_value_1 = valuegetter(inner_before_last,'fsmName',secondary_metric_1,'finalValue') 

                    secondary_this_1 = getPerChange(secondary_last_value_1, secondary_this_value_1) 
                    secondary_last_1 = getPerChange(secondary_before_last_value_1, secondary_last_value_1)

                    secondary_metric_2 = list(result_growth.keys())[1]
                    secondary_this_value_2 = valuegetter(inner_this,'fsmName',secondary_metric_2,'finalValue')  
                    secondary_last_value_2 = valuegetter(inner_last,'fsmName',secondary_metric_2,'finalValue')  
                    secondary_before_last_value_2 = valuegetter(inner_before_last,'fsmName',secondary_metric_2,'finalValue')

                    secondary_this_2 = getPerChange(secondary_last_value_2, secondary_this_value_2) 
                    secondary_last_2 = getPerChange(secondary_before_last_value_2, secondary_last_value_2)

                    percentage_growth_move, summary_growth_move = primary_secondary_double_move(primary_metric_name, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name,secondary_metrics=secondary_metrics_value)
        #             print(percentage_growth_move)

                growth_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                if (len(result_growth) == 1) or (len(result_growth) == 2):
                    moves['Monthly Growth %'] = insightDict(
                                    'Monthly Growth %', 
                                    percentage_growth_move, 
                                    summary_growth_move, 
                                    growth_move_direction,
                                    0
                                )

        if midMonthData:
            # Mid Monthly Move
            primary_metric = "Monthly Growth %"
            primary_metric_name = "Monthly Growth"
            primary_this = valuegetter(mid_outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_this,'name',primary_metric,'value') 

            growth_rate_change = getPerChange(primary_last, primary_this)
            
            total_sales_channels = (inner_df[inner_df['fsName'] == 'Sales']).groupby('fsmName')['finalValue'].sum().reset_index()
            count_total_sales_channels = total_sales_channels[total_sales_channels['finalValue'] > 0]['fsmName'].count()

            if count_total_sales_channels > 1:
                secondary_metrics_value = True
            else:
                secondary_metrics_value = False

            mt_effected = affected_metrics(primary_change_mid_dict, growth_rate_change, ['Gross Profit %', 'CM1 %', 'CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        
            
            if abs(growth_rate_change) >= growth_threshold:

                result_growth = growth_subset_df(financial_statement_id_mapping,mid_inner_this,mid_inner_last,inner_before_last,'','Monthly Growth %',growth_rate_change)
                if len(result_growth) == 1:
                    secondary_metric = list(result_growth.keys())[0]

                    secondary_this_value = valuegetter(mid_inner_this,'fsmName',secondary_metric,'finalValue')  
                    secondary_last_value = valuegetter(mid_inner_last,'fsmName',secondary_metric,'finalValue')  
                    secondary_before_last_value = valuegetter(mid_inner_before_last,'fsmName',secondary_metric,'finalValue') 

                    secondary_this = getPerChange(secondary_last_value, secondary_this_value) 
                    secondary_last = getPerChange(secondary_before_last_value, secondary_last_value)  

                    mid_percentage_growth_move, mid_summary_growth_move = primary_secondary_single_move(primary_metric_name, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name,moveType='MidMonth',secondary_metrics=secondary_metrics_value)
                #             print(percentage_growth_move)

                if len(result_growth) == 2:
                    secondary_metric_1 = list(result_growth.keys())[0]
                    secondary_this_value_1 = valuegetter(mid_inner_this,'fsmName',secondary_metric_1,'finalValue')  
                    secondary_last_value_1 = valuegetter(mid_inner_last,'fsmName',secondary_metric_1,'finalValue')  
                    secondary_before_last_value_1 = valuegetter(mid_inner_before_last,'fsmName',secondary_metric_1,'finalValue') 

                    secondary_this_1 = getPerChange(secondary_last_value_1, secondary_this_value_1) 
                    secondary_last_1 = getPerChange(secondary_before_last_value_1, secondary_last_value_1)

                    secondary_metric_2 = list(result_growth.keys())[1]
                    secondary_this_value_2 = valuegetter(mid_inner_this,'fsmName',secondary_metric_2,'finalValue')  
                    secondary_last_value_2 = valuegetter(mid_inner_last,'fsmName',secondary_metric_2,'finalValue')  
                    secondary_before_last_value_2 = valuegetter(mid_inner_before_last,'fsmName',secondary_metric_2,'finalValue')

                    secondary_this_2 = getPerChange(secondary_last_value_2, secondary_this_value_2) 
                    secondary_last_2 = getPerChange(secondary_before_last_value_2, secondary_last_value_2)

                    mid_percentage_growth_move, mid_summary_growth_move = primary_secondary_double_move(primary_metric_name, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name,moveType='MidMonth',secondary_metrics=secondary_metrics_value)
                #             print(percentage_growth_move)

                mid_growth_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                if (len(result_growth) == 1) or (len(result_growth) == 2):
                    moves['Monthly Growth %'] = insightDict(
                                'Monthly Growth %', 
                                mid_percentage_growth_move, 
                                mid_summary_growth_move, 
                                mid_growth_move_direction,
                                1
                            )

    except Exception as e:
        print(f'Error in Monthly Growth % Move: {e}')
    
    try:
        # Gross Profit % Move
        
        if not midMonthData:
            gp_this = valuegetter(outer_this,'name','Gross Profit %','value') 
            gp_last = valuegetter(outer_last,'name','Gross Profit %','value') 
            sales_this = valuegetter(outer_this,'name','Net Sales','value') 
            sales_last = valuegetter(outer_last,'name','Net Sales','value')
            spend_this = valuegetter(outer_this,'name','Ad Spend','value') 
            spend_last = valuegetter(outer_last,'name','Ad Spend','value')
            cogs_this = valuegetter(outer_this,'name','Cost of Goods Sold','value') 
            cogs_last = valuegetter(outer_last,'name','Cost of Goods Sold','value')

            gp_change_pct = getPerChange(gp_last, gp_this)
            sales_change_pct = getPerChange(sales_last, sales_this)
            spend_change_pct = getPerChange(spend_last, spend_this)
            cogs_change_pct = getPerChange(cogs_last, cogs_this) 

            mt_effected = affected_metrics(primary_change_dict, gp_change_pct, ['CM1 %', 'CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        

            if abs(gp_change_pct) >= gp_threshold:

                gp_percentage_move, gp_summary_move = cogs_sales_move(gp_change_pct,gp_last,gp_this,sales_change_pct,sales_last,sales_this,cogs_change_pct,cogs_last,cogs_this,last_month_name,mt_effected,5)
                if gp_percentage_move != '':

                    gp_move_direction = 'increase' if gp_this > gp_last else 'decrease'

                    moves['Gross Profit %'] = insightDict(
                                    'Gross Profit %', 
                                    gp_percentage_move, 
                                    gp_summary_move,
                                    gp_move_direction,
                                    0
                                )
                    
        if midMonthData:
            gp_this = valuegetter(mid_outer_this,'name','Gross Profit %','value') 
            gp_last = valuegetter(outer_this,'name','Gross Profit %','value') 
            
            sales_this = valuegetter(mid_outer_this,'name','Net Sales','value') 
            sales_last = valuegetter(mid_outer_last,'name','Net Sales','value')
            spend_this = valuegetter(mid_outer_this,'name','Ad Spend','value') 
            spend_last = valuegetter(mid_outer_last,'name','Ad Spend','value')
            cogs_this = valuegetter(mid_outer_this,'name','Cost of Goods Sold','value') 
            cogs_last = valuegetter(mid_outer_last,'name','Cost of Goods Sold','value')

            gp_change_pct = getPerChange(gp_last, gp_this)
            sales_change_pct = getPerChange(sales_last, sales_this)
            spend_change_pct = getPerChange(spend_last, spend_this)
            cogs_change_pct = getPerChange(cogs_last, cogs_this) 

            mt_effected = affected_metrics(primary_change_mid_dict, gp_change_pct, ['CM1 %', 'CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        
            
            if abs(gp_change_pct) >= gp_threshold:

                gp_percentage_move, gp_summary_move = cogs_sales_move(gp_change_pct,gp_last,gp_this,sales_change_pct,sales_last,sales_this,cogs_change_pct,cogs_last,cogs_this,last_month_name,mt_effected,5,moveType='Mid Month')
                if gp_percentage_move != '':

                    gp_move_direction = 'increase' if gp_this > gp_last else 'decrease'

                    moves['Gross Profit %'] = insightDict(
                                    'Gross Profit %', 
                                    gp_percentage_move, 
                                    gp_summary_move,
                                    gp_move_direction,
                                    1
                                )
    except Exception as e:
        print(f'Error in Gross Profit % Move: {e}')
    
    try:
        if not midMonthData:
            # CM1 % Move
            primary_metric = "CM1 %"
            primary_this = valuegetter(outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_last,'name',primary_metric,'value')

            cm1_rate_change = getPerChange(primary_last, primary_this)

            mt_effected = affected_metrics(primary_change_dict, cm1_rate_change, ['CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        
            
            if abs(cm1_rate_change) >= cm1_threshold:

                result_cm_1 = subset_df(financial_statement_id_mapping,inner_this,inner_last,'Gross Profit %','CM1',cm1_rate_change)
                if len(result_cm_1) == 1:
                    secondary_metric = list(result_cm_1.keys())[0]
                    secondary_this = inner_this[inner_this['fsmName'] == secondary_metric]['finalValue'].iloc[0]
                    secondary_last = inner_last[inner_last['fsmName'] == secondary_metric]['finalValue'].iloc[0]

                    percentage_cm1_move, summary_cm1_move = primary_secondary_single_move(primary_metric, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name)
        #             print(percentage_cm1_move)

                if len(result_cm_1) == 2: 
                    secondary_metric_1 = list(result_cm_1.keys())[0]
                    secondary_this_1 = inner_this[inner_this['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                    secondary_last_1 = inner_last[inner_last['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                    secondary_metric_2 = list(result_cm_1.keys())[1]
                    secondary_this_2 = inner_this[inner_this['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]
                    secondary_last_2 = inner_last[inner_last['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]

                    percentage_cm1_move, summary_cm1_move = primary_secondary_double_move(primary_metric, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name)
        #             print(percentage_cm1_move)

                cm1_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                if (len(result_cm_1) == 1) or (len(result_cm_1) == 2):
                    moves['CM1 %'] = insightDict(
                                    'CM1 %', 
                                    percentage_cm1_move, 
                                    summary_cm1_move,
                                    cm1_move_direction,
                                    0
                                )

            
        if midMonthData:
            # Mid Monthly Move
            primary_metric = "CM1 %"
            primary_this = valuegetter(mid_outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_this,'name',primary_metric,'value')

            cm1_rate_change = getPerChange(primary_last, primary_this)
            
            mt_effected = affected_metrics(primary_change_mid_dict, cm1_rate_change, ['CM2 %', 'EBITDA Margin', 'Net Cash Margin'])                        
            
            if abs(cm1_rate_change) >= cm1_threshold:

                result_cm1 = subset_df(financial_statement_id_mapping,mid_inner_this,mid_inner_last,'Gross Profit %','CM1',cm1_rate_change)
                if len(result_cm1) == 1:
                    secondary_metric = list(result_cm1.keys())[0]
                    secondary_this = valuegetter(mid_inner_this,'fsmName',secondary_metric,'finalValue')
                    secondary_last = valuegetter(mid_inner_last,'fsmName',secondary_metric,'finalValue')      

                    mid_percentage_cm1_move, mid_summary_cm1_move  = primary_secondary_single_move(primary_metric, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name,moveType='MidMonth')

                if len(result_cm1) == 2:
                    secondary_metric_1 = list(result_cm1.keys())[0]
                    secondary_this_1 = valuegetter(mid_inner_this,'fsmName',secondary_metric_1,'finalValue') 
                    secondary_last_1 = valuegetter(mid_inner_last,'fsmName',secondary_metric_1,'finalValue')
                    secondary_metric_2 = list(result_cm1.keys())[1]
                    secondary_this_2 = valuegetter(mid_inner_this,'fsmName',secondary_metric_2,'finalValue')
                    secondary_last_2 = valuegetter(mid_inner_last,'fsmName',secondary_metric_2,'finalValue')

                    mid_percentage_cm1_move, mid_summary_cm1_move  = primary_secondary_double_move(primary_metric, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name,moveType='MidMonth')

                mid_cm1_move_direction = 'increase' if primary_this > primary_last else 'decrease'
                
                if (len(result_cm1) == 1) or (len(result_cm1) == 2):
                    moves['CM1 %'] = insightDict(
                                'CM1 %', 
                                mid_percentage_cm1_move, 
                                mid_summary_cm1_move,
                                mid_cm1_move_direction,
                                1
                            )            
    except Exception as e:
        print(f'Error in CM1 % Move: {e}')
        
    
    
    try:
        # CM2 % Move
        def sumOfColumnValue(df,colList):
            conditions = df['name'].isin(colList)
            sumValue = df.loc[conditions, 'value'].sum()
            return sumValue

        if not midMonthData:
            sales_spend_diff = sumOfColumnValue(outer_this,['Net Sales', 'Ad Spend']) - sumOfColumnValue(outer_last,['Net Sales', 'Ad Spend'])
            rest_cm2_diff = sumOfColumnValue(outer_this,['Platform Commission', 'Additional Cost']) - sumOfColumnValue(outer_last,['Platform Commission', 'Additional Cost'])

            if sales_spend_diff > rest_cm2_diff:

                cm2_this = valuegetter(outer_this,'name','CM2 %','value') 
                cm2_last = valuegetter(outer_last,'name','CM2 %','value') 
                sales_this = valuegetter(outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(outer_last,'name','Net Sales','value')
                spend_this = valuegetter(outer_this,'name','Ad Spend','value') 
                spend_last = valuegetter(outer_last,'name','Ad Spend','value')

                cm2_change_pct = getPerChange(cm2_last, cm2_this)
                sales_change_pct = getPerChange(sales_last, sales_this)
                spend_change_pct = getPerChange(spend_last, spend_this)

                mt_effected = affected_metrics(primary_change_dict, cm2_change_pct, ['EBITDA Margin', 'Net Cash Margin'])
                
                if abs(cm2_change_pct) >= cm2_threshold:

                    cm2_percentage_move, cm2_summary_move = sales_spend_move(cm2_change_pct,cm2_last,cm2_this,sales_change_pct,sales_last,sales_this,spend_change_pct,spend_last,spend_this,last_month_name,mt_effected,5)
                    if cm2_percentage_move != '':
                        cm2_move_direction = 'increase' if cm2_this > cm2_last else 'decrease'

                        moves['CM2 %'] = insightDict(
                                        'CM2 %', 
                                        cm2_percentage_move, 
                                        cm2_summary_move, 
                                        cm2_move_direction,
                                        0
                                    )
            else:
                primary_metric = "CM2 %"
                primary_this = valuegetter(outer_this,'name',primary_metric,'value') 
                primary_last = valuegetter(outer_last,'name',primary_metric,'value')

                cm2_rate_change = getPerChange(primary_last, primary_this)
                mt_effected = affected_metrics(primary_change_dict, cm2_rate_change, ['EBITDA Margin', 'Net Cash Margin'])  

                if abs(cm2_rate_change) >= cm2_threshold:

                    result_cm_2 = subset_df(financial_statement_id_mapping,inner_this,inner_last,'CM1 %','CM2',cm2_rate_change)
                    if len(result_cm_2) == 1:
                        secondary_metric = list(result_cm_2.keys())[0]
                        secondary_this = inner_this[inner_this['fsmName'] == secondary_metric]['finalValue'].iloc[0]
                        secondary_last = inner_last[inner_last['fsmName'] == secondary_metric]['finalValue'].iloc[0]

                        percentage_cm2_move, summary_cm2_move = primary_secondary_single_move(primary_metric, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name)

                    if len(result_cm_2) == 2: 
                        secondary_metric_1 = list(result_cm_2.keys())[0]
                        secondary_this_1 = inner_this[inner_this['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                        secondary_last_1 = inner_last[inner_last['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                        secondary_metric_2 = list(result_cm_2.keys())[1]
                        secondary_this_2 = inner_this[inner_this['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]
                        secondary_last_2 = inner_last[inner_last['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]                      
                        
                        percentage_cm2_move, summary_cm2_move = primary_secondary_double_move(primary_metric, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name)

                    cm2_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                    if (len(result_cm_2) == 1) or (len(result_cm_2) == 2):
                        moves['CM2 %'] = insightDict(
                                        'CM2 %', 
                                        percentage_cm2_move,
                                        summary_cm2_move,
                                        cm2_move_direction,
                                        0
                                    )
                
        if midMonthData:
            sales_spend_diff = sumOfColumnValue(mid_outer_this,['Net Sales', 'Ad Spend']) - sumOfColumnValue(mid_outer_last,['Net Sales', 'Ad Spend'])
            rest_cm2_diff = sumOfColumnValue(mid_outer_this,['Platform Commission', 'Additional Cost']) - sumOfColumnValue(mid_outer_last,['Platform Commission', 'Additional Cost'])

            if sales_spend_diff > rest_cm2_diff:

                cm2_this = valuegetter(mid_outer_this,'name','CM2 %','value') 
                cm2_last = valuegetter(outer_this,'name','CM2 %','value') 
                
                sales_this = valuegetter(mid_outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(mid_outer_last,'name','Net Sales','value')
                spend_this = valuegetter(mid_outer_this,'name','Ad Spend','value') 
                spend_last = valuegetter(mid_outer_last,'name','Ad Spend','value')

                cm2_change_pct = getPerChange(cm2_last, cm2_this)
                sales_change_pct = getPerChange(sales_last, sales_this)
                spend_change_pct = getPerChange(spend_last, spend_this)
                
                mt_effected = affected_metrics(primary_change_mid_dict, cm2_change_pct, ['EBITDA Margin', 'Net Cash Margin'])                        

                if abs(cm2_change_pct) >= cm2_threshold:

                    cm2_percentage_move, cm2_summary_move = sales_spend_move(cm2_change_pct,cm2_last,cm2_this,sales_change_pct,sales_last,sales_this,spend_change_pct,spend_last,spend_this,last_month_name,mt_effected,5,moveType='Mid Month')
                    if cm2_percentage_move != '':
                        cm2_move_direction = 'increase' if cm2_this > cm2_last else 'decrease'

                        moves['CM2 %'] = insightDict(
                                        'CM2 %', 
                                        cm2_percentage_move, 
                                        cm2_summary_move,
                                        cm2_move_direction,
                                        1
                                    )
            else:
                primary_metric = "CM2 %"
                primary_this = valuegetter(mid_outer_this,'name',primary_metric,'value') 
                primary_last = valuegetter(outer_this,'name',primary_metric,'value')

                cm2_rate_change = getPerChange(primary_last, primary_this)

                mt_effected = affected_metrics(primary_change_mid_dict, cm2_rate_change, ['EBITDA Margin', 'Net Cash Margin'])                        
                
                if abs(cm2_rate_change) >= cm2_threshold:

                    result_cm2 = subset_df(financial_statement_id_mapping,mid_inner_this,mid_inner_last,'CM1 %','CM2',cm2_rate_change)
                    if len(result_cm2) == 1:
                        secondary_metric = list(result_cm2.keys())[0]
                        secondary_this = valuegetter(mid_inner_this,'fsmName',secondary_metric,'finalValue')
                        secondary_last = valuegetter(mid_inner_last,'fsmName',secondary_metric,'finalValue')      

                        mid_percentage_cm2_move, mid_summary_cm2_move = primary_secondary_single_move(primary_metric, primary_this, primary_last, secondary_metric, secondary_this, secondary_last, mt_effected, last_month_name,moveType='MidMonth')

                    if len(result_cm2) == 2:
                        secondary_metric_1 = list(result_cm2.keys())[0]
                        secondary_this_1 = valuegetter(mid_inner_this,'fsmName',secondary_metric_1,'finalValue') 
                        secondary_last_1 = valuegetter(mid_inner_last,'fsmName',secondary_metric_1,'finalValue')
                        secondary_metric_2 = list(result_cm2.keys())[1]
                        secondary_this_2 = valuegetter(mid_inner_this,'fsmName',secondary_metric_2,'finalValue')
                        secondary_last_2 = valuegetter(mid_inner_last,'fsmName',secondary_metric_2,'finalValue')

                        mid_percentage_cm2_move, mid_summary_cm2_move = primary_secondary_double_move(primary_metric, primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, mt_effected, last_month_name,moveType='MidMonth')

                    mid_cm2_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                    if (len(result_cm2) == 1) or (len(result_cm2) == 2):
                        moves['CM2 %'] = insightDict(
                                    'CM2 %', 
                                    mid_percentage_cm2_move, 
                                    mid_summary_cm2_move,
                                    mid_cm2_move_direction,
                                    1
                                )            

    except Exception as e:
        print(f'Error in CM2 % Move: {e}')        
#     return inner_df  
    try:
        if not midMonthData:
            # EBITDA Margin Move
            primary_metric = "EBITDA Margin"
            primary_this = valuegetter(outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_last,'name',primary_metric,'value')
            
            opex_this = valuegetter(outer_this,'name','Operating Expenses','value')
            opex_last = valuegetter(outer_last,'name','Operating Expenses','value')
            
            if opex_this > 0 and opex_last > 0:  
            
                sales_this = valuegetter(outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(outer_last,'name','Net Sales','value')

                ebitda_change = getPerChange(primary_last, primary_this)
                if abs(ebitda_change) >= ebitda_threshold:

                    result_ebitda = subset_df(financial_statement_id_mapping,inner_this,inner_last,'CM2 %','EBITDA',ebitda_change)

                    if len(result_ebitda) == 1:
                        secondary_metric = list(result_ebitda.keys())[0]
                        secondary_this = inner_this[inner_this['fsmName'] == secondary_metric]['finalValue'].iloc[0]
                        secondary_last = inner_last[inner_last['fsmName'] == secondary_metric]['finalValue'].iloc[0]

                        percentage_ebitda_move, summary_ebitda_move = ebitda_single_move(primary_this, primary_last, secondary_metric, secondary_this, secondary_last, sales_this, sales_last, last_month_name)
            #             print(percentage_ebitda_move)

                    if len(result_ebitda) == 2:
                        secondary_metric_1 = list(result_ebitda.keys())[0]
                        secondary_this_1 = inner_this[inner_this['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                        secondary_last_1 = inner_last[inner_last['fsmName'] == secondary_metric_1]['finalValue'].iloc[0]
                        secondary_metric_2 = list(result_ebitda.keys())[1]
                        secondary_this_2 = inner_this[inner_this['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]
                        secondary_last_2 = inner_last[inner_last['fsmName'] == secondary_metric_2]['finalValue'].iloc[0]

                        percentage_ebitda_move, summary_ebitda_move = ebitda_double_move(primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, sales_this, sales_last, last_month_name)
            #             print(percentage_ebitda_move)

                    ebitda_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                    if (len(result_ebitda) == 1) or (len(result_ebitda) == 2):
                        moves['EBITDA Margin'] = insightDict(
                                        'EBITDA Margin', 
                                        percentage_ebitda_move, 
                                        summary_ebitda_move,
                                        ebitda_move_direction,
                                        0
                                    )


        if midMonthData:
            # Mid Monthly Move
            primary_metric = "EBITDA Margin"
            primary_this = valuegetter(mid_outer_this,'name',primary_metric,'value') 
            primary_last = valuegetter(outer_this,'name',primary_metric,'value')

            opex_this = valuegetter(mid_outer_this,'name','Operating Expenses','value') 
            opex_last = valuegetter(mid_outer_last,'name','Operating Expenses','value')
            
            if opex_this > 0 and opex_last > 0:
            
                sales_this = valuegetter(mid_outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(mid_outer_last,'name','Net Sales','value')

                ebitda_change = getPerChange(primary_last, primary_this)

                if abs(ebitda_change) >= ebitda_threshold:

                    result_ebitda = subset_df(financial_statement_id_mapping,mid_inner_this,mid_inner_last,'CM2 %','EBITDA',ebitda_change)
                    if len(result_ebitda) == 1:
                        secondary_metric = list(result_ebitda.keys())[0]
                        secondary_this = valuegetter(mid_inner_this,'fsmName',secondary_metric,'finalValue')
                        secondary_last = valuegetter(mid_inner_last,'fsmName',secondary_metric,'finalValue') 

                        mid_percentage_ebitda_move, mid_summary_ebitda_move  = ebitda_single_move(primary_this, primary_last, secondary_metric, secondary_this, secondary_last, sales_this, sales_last, last_month_name, moveType='MidMonth')

                    if len(result_ebitda) == 2:
                        secondary_metric_1 = list(result_ebitda.keys())[0]
                        secondary_this_1 = valuegetter(mid_inner_this,'fsmName',secondary_metric_1,'finalValue') 
                        secondary_last_1 = valuegetter(mid_inner_last,'fsmName',secondary_metric_1,'finalValue')
                        secondary_metric_2 = list(result_ebitda.keys())[1]
                        secondary_this_2 = valuegetter(mid_inner_this,'fsmName',secondary_metric_2,'finalValue')
                        secondary_last_2 = valuegetter(mid_inner_last,'fsmName',secondary_metric_2,'finalValue')

                        mid_percentage_ebitda_move, mid_summary_ebitda_move  = ebitda_double_move(primary_this, primary_last, secondary_metric_1, secondary_this_1, secondary_last_1, secondary_metric_2, secondary_this_2, secondary_last_2, sales_this, sales_last, last_month_name, moveType='MidMonth')

                    mid_ebitda_move_direction = 'increase' if primary_this > primary_last else 'decrease'

                    if (len(result_ebitda) == 1) or (len(result_ebitda) == 2):
                        moves['EBITDA Margin'] = insightDict(
                                    'EBITDA Margin', 
                                    mid_percentage_ebitda_move, 
                                    mid_summary_ebitda_move,
                                    mid_ebitda_move_direction,
                                    1
                                )            
    except Exception as e:
        print(f'Error in EBITDA Margin Move: {e}')
        
    
    try:
        if not midMonthData:
            # Net Cash Margin Move
            net_cash_metric = 'Net Cash Margin'
            net_cash_this = valuegetter(outer_this,'name',net_cash_metric,'value') 
            net_cash_last = valuegetter(outer_last,'name',net_cash_metric,'value')
            
            loan_this = valuegetter(outer_this,'name','Loan Servicing','value')
            loan_last = valuegetter(outer_last,'name','Loan Servicing','value')
            
            if loan_this > 0 and loan_last > 0: 
            
                sales_this = valuegetter(outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(outer_last,'name','Net Sales','value')

                net_cash_change = getPerChange(net_cash_last, net_cash_this)
                if abs(net_cash_change) >= net_cash_threshold:
                    percentage_net_cash_move, summary_net_cash_move = net_cash_move(net_cash_this , net_cash_last, net_cash_change, loan_this, loan_last, sales_this , sales_last, last_month_name )

                    net_cash_move_direction = 'increase' if net_cash_this > net_cash_last else 'decrease'

                    moves['Net Cash Margin'] = insightDict(
                                    'Net Cash Margin', 
                                    percentage_net_cash_move, 
                                    summary_net_cash_move,
                                    net_cash_move_direction,
                                    0
                                )

            
        if midMonthData:
            # Mid Monthly Move
            net_cash_metric = 'Net Cash Margin'
            net_cash_this = valuegetter(mid_outer_this,'name',net_cash_metric,'value') 
            net_cash_last = valuegetter(outer_this,'name',net_cash_metric,'value')
            
            loan_this = valuegetter(mid_outer_this,'name','Loan Servicing','value') 
            loan_last = valuegetter(mid_outer_last,'name','Loan Servicing','value')

            if loan_this > 0 and loan_last > 0: 
            
                sales_this = valuegetter(mid_outer_this,'name','Net Sales','value') 
                sales_last = valuegetter(mid_outer_last,'name','Net Sales','value')

                net_cash_change = getPerChange(net_cash_last, net_cash_this)

                if abs(net_cash_change) >= net_cash_threshold:
                    mid_percentage_net_cash_move, mid_summary_net_cash_move  = net_cash_move(net_cash_this , net_cash_last, net_cash_change, loan_this, loan_last, sales_this , sales_last, last_month_name , moveType='MidMonth')

                    mid_net_cash_move_direction = 'increase' if net_cash_this > net_cash_last else 'decrease'

                    moves['Net Cash Margin'] = insightDict(
                                'Net Cash Margin', 
                                mid_percentage_net_cash_move, 
                                mid_summary_net_cash_move,
                                mid_net_cash_move_direction,
                                1
                            )            
    except Exception as e:
        print(f'Error in Net Cash Margin Move: {e}')
        
    for move in financialStatementMoves:
        label = move["label"]
        if label in moves:
            move.update(moves[label])

    return financialStatementMoves