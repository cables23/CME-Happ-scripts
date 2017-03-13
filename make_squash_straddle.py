# Standards
import os

import numpy as np
import pandas as pd

import webbrowser

# Locals
from tz_interface import tz_dropbox, tz_gmail
from tz_interface.tz_happ import tz_happ_auto

DROPBOX_PATH = tz_dropbox.toStr()


####### CONSTANTS ##################################################################################


TOTAL_LBL = 'Grand Total'
TOTAL_MIDCURVE_LBL = 'MC Total'
TOTAL_LESS_NEXT_LBL = 'w/o Next'

XLSX_FILE_PATH = os.path.join(DROPBOX_PATH, 'Daily Runs', 'Squash_Straddle_report.xlsx')
HTML_FILE_PATH = os.path.join(DROPBOX_PATH, 'Daily Runs', 'Squash_Straddle_report.html')

ADDT_COLS = ['g_code', 'ATMS_NEW', 'PS_NEW', 'CS_NEW', 'PS_SQ', 'CS_SQ', 'PS_R', 'CS_R']

REMOVE_ERRORS = lambda x: x if np.isfinite(x) else 0

MC_NAME_DICT = {'S': "Short", 'G': "Green", 'B': "Blue", 'F': "Gold", 'I': "Purple"}

MC_DIFF_DICT = {'S': 1, 'G': 2, 'B': 3, 'F': 4, 'I': 5}


########## FUNCTIONS ###############################################################################


def df_from_pwa_lines(raw_clipboard_lines):

    # Extracting data from the pywinauto risk report data which comes in as clipboard lines

    data_rows_potential = [line for line in raw_clipboard_lines if '/' in line]

    max_row_length = max([len(x) for x in data_rows_potential])

    data_rows_filtered = [line for line in data_rows_potential if len(line) == max_row_length]

    output_df = pd.DataFrame([line.split() for line in data_rows_filtered])

    # Extracting the column names from the pywin data

    header_rows_potential = [line for line in raw_clipboard_lines if '/' not in line]

    _, header_row_filtered = max([(len(x), x) for x in header_rows_potential])

    column_names = [x.strip(' ') for x in header_row_filtered.split('  ') if x.strip() != '']

    column_names.insert(0, 'MONTH')
    column_names[-1] = column_names[-1].replace('\r', '')

    output_df.columns = column_names
    output_df.rename(columns={'PUT SLP': 'PS', 'CLL SLP': 'CS'}, inplace=True)

    output_df.set_index('MONTH', inplace=True)

    return output_df


def get_g_code(df_row):
    # Returns the mid curve gorup-code for display purposes

    if df_row['is_front']:
        return 'Front'

    else:
        return MC_NAME_DICT[df_row['happ_g_code']]


def get_exp_my_code(df_row):
    # Makes month-year code based on midcurve

    if df_row['is_front']:
        return df_row['exp_m_code'] + str(df_row['u_yr'])

    else:
        int_year = int(df_row['u_yr']) - MC_DIFF_DICT[df_row['happ_g_code']]
        return df_row['exp_m_code'] + str(int_year % 10)


def make_unique(original_list):
    # returns unique list without affecting order of original
    print(original_list)
    unique_list = []

    [unique_list.append(obj) for obj in original_list if obj not in unique_list]
    print(unique_list)
    return unique_list


def month_group_pivot(pre_pivot_df, pivot_value_col):

    indices_for_pivot = make_unique(pre_pivot_df['exp_my_code'])
    columns_for_pivot = make_unique(pre_pivot_df['g_code'])

    pivoted_df = pd.pivot_table(pre_pivot_df, values=pivot_value_col,
                                index='exp_my_code', columns='g_code', aggfunc=np.sum, fill_value=0,
                                margins=True, margins_name=TOTAL_LBL).round()
    pivoted_df.index.names = [pivot_value_col]

    mc_groups_in_df = [col for col in pivoted_df.columns if col in MC_NAME_DICT.values()]
    pivoted_df[TOTAL_MIDCURVE_LBL] = pivoted_df[mc_groups_in_df].sum(axis=1)

    pivoted_df = pivoted_df.reindex(columns=columns_for_pivot + [TOTAL_LBL, TOTAL_MIDCURVE_LBL],
                                    index=indices_for_pivot + [TOTAL_LBL])

    pivoted_df.loc[TOTAL_LESS_NEXT_LBL] = (pivoted_df[1:-1].sum(axis=0))

    return pivoted_df


def output_to_excel(list_dfs, xls_path):

    writer = pd.ExcelWriter(xls_path)
    df_start_col = 0

    for n, df in enumerate(list_dfs):

        df.to_excel(writer, 'Sheet1', startcol=df_start_col, startrow=2)

        df_start_col += len(df.columns) + 2

    writer.save()


####### SCRIPT #####################################################################################


def run():

    # Pulling clipboard lines from happ and extracting raw data
    NEW_raw, SQ_raw = tz_happ_auto.get_account_risk_reports([tz_happ_auto.DOWN_TAPS['NEW'],
                                                             tz_happ_auto.DOWN_TAPS['SQ']])
    NEW_df = df_from_pwa_lines(NEW_raw)
    SQ_df = df_from_pwa_lines(SQ_raw)

    # Merging into one df
    dat = NEW_df.join(SQ_df, how='left', lsuffix='_NEW', rsuffix='_SQ').convert_objects(1, 1, 1)

    # Calculating equivalent put and call slopes
    dat['PS_R'] = 1000 * (dat['PS_NEW'] / dat['PS_SQ']).map(REMOVE_ERRORS)
    dat['CS_R'] = -1000 * (dat['CS_NEW'] / dat['CS_SQ']).map(REMOVE_ERRORS)

    # Helper columns for grouping
    dat['u_yr'] = dat.index.map(lambda x: x[1:2])           # underlying year
    dat['is_front'] = dat.index.map(lambda x: len(x) == 4)  # month code (U, V, SU, GU)
    dat['happ_g_code'] = dat.index.map(lambda x: x[3])      # group
    dat['exp_m_code'] = dat.index.map(lambda x: x[-1])      # month expiration (U, V, X, Z)

    # Pivot Table Columns
    dat['g_code'] = dat.apply(get_g_code, axis=1)           # group code
    dat['exp_my_code'] = dat.apply(get_exp_my_code, axis=1) # expiration month-year
    #print(dat['g_code'])
    #print(dat['exp_my_code'])
    #print(dat)
    # pivot tables
    straddle_df = month_group_pivot(dat, 'ATMS_NEW')
    putslope_df = month_group_pivot(dat, 'PS_R')
    cllslope_df = month_group_pivot(dat, 'CS_R')

    # output to excel
    output_to_excel([dat[ADDT_COLS], straddle_df, putslope_df, cllslope_df],
                    xls_path=XLSX_FILE_PATH)

    # output to html file and open file in browser

    report_names = ['Straddle', 'Put Slope', 'Call Slope', 'All Data']
    report_tables = [straddle_df, putslope_df, cllslope_df, dat[ADDT_COLS]]

    html_code = ''

    for report_name, report_table in zip(report_names, report_tables):
        html_code += tz_gmail.html_table(report_name, report_table)

    with open(HTML_FILE_PATH, 'w') as html_file:
        html_file.write(tz_gmail.html_for_report(html_code))

    webbrowser.open('file://' + HTML_FILE_PATH)


if __name__ == '__main__':
    run()
