# Standards
import sys
import os
import pandas as pd
import datetime

# Locals
from tz_interface import tz_dropbox, tz_files, tz_gmail, tz_clearing

############### CONSTANTS ##########################################################################

CLEARING_SOURCE = 'wedPos'
REP_DESC = 'Position'

SUB_DT_FMT = "%a, %m/%d/%Y, %I:%M %p"

TITLE_NEW = 'NEW, NEWS, EXP2 & FUT'
TITLE_NYCO = 'NYCO, NSPEC & EXP (OHA80113)'

############### EMAIL SETTINGS #####################################################################

EMAIL_SENDER = 'reports@tenzancapital.com'

if 'p' in sys.argv:
    EMAIL_RECIPIENTS_STATEMENT = ['dave@tenzancapital.com',
                                  'nikolai@tenzancapital.com']
    EMAIL_RECIPIENTS_SUMMARY = ['dave@tenzancapital.com',
                                'catherine.moseley@wedbush.com',
                                'nikolai@tenzancapital.com']

else:
    EMAIL_RECIPIENTS_STATEMENT = ['dave@tenzancapital.com']
    EMAIL_RECIPIENTS_SUMMARY = ['dave@tenzancapital.com']

####### COMMON FILE PATH STUFF #####################################################################

DROPBOX_PATH = tz_dropbox.toStr()

TEN_POS_DIR = os.path.join(DROPBOX_PATH, 'pos')

WB_POS_PATH_TEMPLATE = os.path.join(TEN_POS_DIR, 'pos', '%sWEDPOS.CSV')

WB_STM_PATH_TEMPLATE = os.path.join(TEN_POS_DIR, 'Wedbush_Statements', 'st%s.pdf')

WB_MNY_PATH_TEMPLATE = os.path.join(TEN_POS_DIR, 'mny', '%sWEDMNY.CSV')

TEN_DB_DIR = os.path.join(DROPBOX_PATH, 'tenDb_raw')

TZ_FILE_TEMPLATE = os.path.join(TEN_DB_DIR, 'posView', '*.CSV')

TZ_RAW_CSV_DIR_TEMPLATE = os.path.join(TEN_DB_DIR, 'tenTables', '*')

######################## FTP DOWNLOAD ##############################################################


def perform_ftp_download():

    timeDay = datetime.datetime.now() - datetime.timedelta(days=1)
    tmDay = timeDay.strftime("%Y%m%d")

    # Wedbush Position
    wb_pos_ftp_fn = os.path.join('Tenzan', 'pos%s.csv') % tmDay
    wb_pos_fn = WB_POS_PATH_TEMPLATE % tmDay

    # Wedbush Statement
    wb_stm_ftp_fn = os.path.join('Tenzan', 'statement.pdf')
    wb_stm_fn = WB_STM_PATH_TEMPLATE % tmDay

    # Wedbush MNY
    wb_mny_ftp_fn = os.path.join('Tenzan', 'mny%s.csv') % tmDay
    wb_mny_fn = WB_MNY_PATH_TEMPLATE % tmDay  # Might not need

    # Dictionary of files to download
    files_ftp_to_local = {wb_pos_ftp_fn: wb_pos_fn,
                          wb_stm_ftp_fn: wb_stm_fn,
                          wb_mny_ftp_fn: wb_mny_fn}

    # Performing the download
    tz_clearing.wedbush_ftp_download(files_ftp_to_local)

######################## FUNCTIONS: EMAIL  #########################################################


def send_statement_email(email_recipients, wb_stm_fn):

    # detail information to be included in email

    wb_stm_tm = tz_files.modified_timestamp(wb_stm_fn)

    details_list_stm = ['Wedbush statement time: %s' % wb_stm_tm,
                        'Wedbush statement file: %s' % os.path.basename(wb_stm_fn)]

    details_str_stm = '\n'.join(details_list_stm)

    tz_gmail.send_mail(EMAIL_SENDER,
                       send_to=email_recipients,
                       subject='Wedbush Stmt %s' % (wb_stm_tm.strftime(SUB_DT_FMT)),
                       text=details_str_stm,
                       files=wb_stm_fn)

#####################################################################################
#####################################################################################
#####################################################################################
#####################################################################################
# COMMON!!!!
#####################################################################################
#####################################################################################
#####################################################################################
#####################################################################################


def send_comparison_email(rec_addresses, final_FIRM, final_NEW, tz_pos_fn, wb_pos_fn):

    # Details included in email

    tz_pos_tm = tz_files.modified_timestamp(tz_pos_fn)

    # Tenzan backup (for informational purpose only)
    tz_raw_dir = tz_files.latest_file_from_template(TZ_RAW_CSV_DIR_TEMPLATE)

    # Wedbush Position
    wb_pos_tm = tz_files.modified_timestamp(wb_pos_fn)

    # Creating details text that will be used in email
    tz_detail_list = ['Tenzan posView time: %s' % tz_pos_tm,
                      'Tenzan posView file: %s' % os.path.basename(tz_pos_fn),
                      'Tenzan snapshot dir: %s' % os.path.basename(os.path.normpath(tz_raw_dir))]

    wb_detail_list = ['%s time: %s' % (CLEARING_SOURCE, wb_pos_tm),
                      '%s file: %s' % (CLEARING_SOURCE, os.path.basename(wb_pos_fn))]

    # Constructing and sending email

    email_subject = '%s %s: %s' % (CLEARING_SOURCE, REP_DESC, wb_pos_tm.strftime(SUB_DT_FMT))

    email_html = tz_gmail.html_tag('ERRORS: %d' % len(final_FIRM), 'h2') + \
        tz_gmail.html_tag('<br>'.join(tz_detail_list), 'p') + \
        tz_gmail.html_tag('<br>'.join(wb_detail_list), 'p') + \
        tz_gmail.html_table('Overall %s' % REP_DESC, final_FIRM) + \
        tz_gmail.html_table(TITLE_NEW, final_NEW)# + \
        #tz_gmail.html_table(TITLE_NYCO, final_NYCO)

    tz_gmail.send_mail(EMAIL_SENDER,
                       send_to=rec_addresses,
                       subject=email_subject,
                       html=email_html)

#####################################################################################
#####################################################################################
#####################################################################################
#####################################################################################


def extract_tz_dfs(tz_pos_fn):

    # Read posView file into a df
    tz_pos_df = pd.read_csv(tz_pos_fn)

    # We want only firm accounts, no test accounts from Happ
    tz_pos_FIRM = tz_pos_df[tz_pos_df['a_type'].isin(['frm'])]

    # Get a subset of columns from Tenzan df
    tz_pos_FIRM_cols = list(tz_clearing.cols2use(CLEARING_SOURCE).keys())  # frm
    tz_pos_FIRM = tz_pos_FIRM[tz_pos_FIRM.columns[tz_pos_FIRM.columns.isin(tz_pos_FIRM_cols)]]

    # Splitting up the accounts in Tenzan df
    tz_pos_NEW = tz_pos_FIRM[tz_pos_FIRM['a_abr'].isin(['NEW', 'NEWS', 'EXP2', 'FUT'])]
    #tz_pos_NYCO = tz_pos_FIRM[tz_pos_FIRM['a_abr'].isin(['NYCO', 'NSPEC', 'EXP'])]

    return tz_pos_FIRM, tz_pos_NEW #, tz_pos_NYCO


def filter_wb_position(wb_pos_fn):

    # Standardize Wedbush pos
    wb_pos_all_df = tz_clearing.stndrdize_wedPos2tenDb(wb_pos_fn)

    # Filter out the group account (9OH)
    wb_pos_FIRM = wb_pos_all_df[wb_pos_all_df['PRR'].isin(['ARBCH', 'OHA01'])]

    # Splitting up the accounts in Wedbush df
    # compare by account: ARB and OHA80110 (split account), and OHA80113
    wb_pos_NEW = wb_pos_FIRM[wb_pos_FIRM['PACCT'].isin([70, 80110])]
    #wb_pos_NYCO_tmp = wb_pos_FIRM[wb_pos_FIRM['PRR'].isin(['OHA01'])]
    #wb_pos_NYCO = wb_pos_NYCO_tmp[wb_pos_NYCO_tmp['PACCT'].isin([80113])]

    return wb_pos_FIRM, wb_pos_NEW #, wb_pos_NYCO


def run():

    # Downloading files form Wedbush FTP
    perform_ftp_download()

    # Determining latest statement and wedPos files names
    wb_pos_fn = tz_files.latest_file_from_template(WB_POS_PATH_TEMPLATE % '*')
    wb_stm_fn = tz_files.latest_file_from_template(WB_STM_PATH_TEMPLATE % '*')

    # emailing statement (internal to TZ)
    send_statement_email(EMAIL_RECIPIENTS_STATEMENT, wb_stm_fn)

    # Identify latest Tenzan posView
    tz_pos_fn = tz_files.latest_file_from_template(TZ_FILE_TEMPLATE)

    # Pulling relevant info from TZ and WB
    tz_pos_FIRM, tz_pos_NEW = extract_tz_dfs(tz_pos_fn)
    wb_pos_FIRM, wb_pos_NEW = filter_wb_position(wb_pos_fn)

    # Performing the Comparison
    comparison_args = (['q'], [0], CLEARING_SOURCE)

    final_pos_FIRM = tz_clearing.tz_wb_compare(wb_pos_FIRM, tz_pos_FIRM, *comparison_args)
    final_pos_NEW = tz_clearing.tz_wb_compare(wb_pos_NEW, tz_pos_NEW, *comparison_args)
    #final_pos_NYCO = tz_clearing.tz_wb_compare(wb_pos_NYCO, tz_pos_NYCO, *comparison_args)

    # emailing comparison (to both TZ and WB)
    send_comparison_email(EMAIL_RECIPIENTS_SUMMARY, final_pos_FIRM,
                          final_pos_NEW, tz_pos_fn, wb_pos_fn)

if __name__ == '__main__':
    run()
