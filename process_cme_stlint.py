# Download and parse CME settles for import into Happ

# Standards
import os
import pandas as pd

from csv import QUOTE_NONE

# Locals
from tz_interface import tz_cme, tz_dropbox

################## CONSTANTS #######################################################################

DROPBOX_PATH = tz_dropbox.toStr()

OUTPUT_FILE_PATH = os.path.join(DROPBOX_PATH, 'settles.txt')

DOWNLOAD_FILE_PATH = os.path.join(DROPBOX_PATH, 'stlint')

CME_PUBLIC_FTP_URL = 'ftp.cmegroup.com'

################## FUNCTIONS #######################################################################


def process_stlint_lines(lines):

    # read the lines and append into a list
    data_nested_lists = []
    key = ''

    for line in lines:

        if len(line.strip()) == 0:

            continue

        tokens = [t for t in line.split() if len(t.strip()) > 0]

        if tokens[0].rstrip(' ') == 'TOTAL':

            key = ''  # If reach total in stlint, do not append

        if key != '':

            if key == 'ED':

                col = [key, tokens[0], 0]  # handles different header from the futures section
                data_nested_lists.append(col + tokens)

            else:  # strike always begins with a 9 or 1

                if (len(tokens[0]) > 3) and ((tokens[0][:1] == '9') or (tokens[0][:1] == '1')):

                    data_nested_lists.append(col + tokens)

                else:

                    key = ''

        if tokens[0] in tz_cme.cme_stlint_lst:
            key = tokens[0]
            col = [tokens[0], tokens[1], tokens[len(tokens) - 1]]

    # Create the dataframe
    return pd.DataFrame(data_nested_lists)

# Handle weeklys by not adding the month after.
# Ex: G1 stays as G1 instead of G1H. G and H still become GH


def weekly(data_df_row):

    if data_df_row['Prod'] in ['S', 'G', 'B', 'F', 'I']:

        # Need to handle for front F which was getting processed as a gold "F"
        if data_df_row['Year'] == data_df_row['undYear']:

            return '"' + data_df_row['Prod'] + '"'

        else:

            return '"' + data_df_row['Prod'] + data_df_row['Month'] + '"'
    else:

        return '"' + data_df_row['Prod'] + '"'

# Returns corresponding underlying for Midcurves. Ex: For shorts, 16 as year of expiration
# returns 17 as underlying


def midcurve_underlying(data_df_row):

    pr = data_df_row['Prod']
    yr = data_df_row['Year']

    if pr in ['ED', 'ZE']:
        mu = yr

    elif pr in ['S', 'S1', 'S2', 'S3', 'S4', 'S5']:
        mu = yr + 1

    elif pr in ['G', 'G1', 'G2', 'G3', 'G4', 'G5']:
        mu = yr + 2

    elif pr in ['B', 'B1', 'B2', 'B3', 'B4', 'B5']:
        mu = yr + 3

    elif pr == 'F':
        mu = yr + 4

    elif pr == 'I':
        mu = yr + 5

    else:
        mu = yr

    return mu

str_strip_right = lambda x: str(x).rstrip('0').rstrip('.')

####################################################################################################
################## SCRIPT START ####################################################################
####################################################################################################


def run():

    # Download the settles file from CME FTP site
    tz_cme.public_ftp_directory_download(local_directory=DROPBOX_PATH,
                                         ftp_directory='settle',
                                         white_list=['stlint'],
                                         force_download=True)

    # create own csv with List
    with open(DOWNLOAD_FILE_PATH, 'r') as f:
        lines = f.readlines()

    data_raw_df = process_stlint_lines(lines)

    # Split up second column into two columns.  Ex: SEP17 becomes SEP and 17
    data_raw_df['mon'] = data_raw_df[1].str[0:3]
    data_raw_df['yr'] = data_raw_df[1].str[3:5].astype(int)

    # Subsetting and renaming columns
    data_df = data_raw_df[[0, 'mon', 'yr', 2, 3, 8]]
    data_df.columns = ['Prod', 'Month', 'Year', 'CP', 'Strike', 'Price']

    # Switch the keys for values to line up for the dataframe
    swap_dict = {v: k for k, v in tz_cme.o_ed_midcurve.items()}
    data_df['Prod'].replace(swap_dict, inplace=True)

    # Replace month with letter.  EX: AUG becomes Q,  SEP becomes U
    data_df['Month'].replace(tz_cme.cme_stlint_month_dic, inplace=True)

    # use function to get the correct underlying for midcurves.  Ex: Shorts
    # that expire in 16, under is 17
    data_df['undYear'] = data_df.apply(midcurve_underlying, axis=1)

    # Fronts, reds and longs changed from ZE
    filt_ZE = data_df['Prod'] == 'ZE'
    data_df.loc[filt_ZE, 'Prod'] = data_df[filt_ZE]['Month']

    # Change underlying to correct month for serials.  Ex: F, G become H. J, K become M, etc.
    data_df['undMonth'] = data_df['Month'].replace(tz_cme.cme_ed_expiration_to_underlying_dict)
    filt_ED = data_df['Prod'] == 'ED'
    data_df.loc[filt_ED, 'undMonth'] = data_df[filt_ED]['Month']

    # first column:
    data_df['Underlying'] = '"ED' + data_df['undMonth'].map(str) + data_df['undYear'].map(str) + '"'

    # Change ED to "".  Happ requires "" for futures
    data_df.loc[filt_ED, 'Prod'] = '""'

    # Change product code from E0,  E2,  etc to S,  G,  B,  etc.
    data_df['HappProd'] = data_df.apply(weekly, axis=1)

    # This is third column of settles.txt Replace CALL with C, etc.
    data_df['CP'].replace({'CALL': '"C"', 'PUT': '"P"', 0: '" "'}, inplace=True)

    # This is a fudge. We do a division in the next couple lines of code to get correct strike format
    # This makes sure we are not dividing by 0. Later we will set this to "" again.
    data_df.loc[data_df['Prod'] == '""', 'Strike'] = 1000

    # Create fourth column, strike for settles.txt
    data_df['X'] = (round(0.08 * data_df['Strike'].astype(int)) / 8).map(str_strip_right)

    # Set strike for futures back to 0
    data_df.loc[data_df['Prod'] == '""', 'X'] = 0

    # Remove trailing 0's from price
    data_df['Price'] = data_df['Price'].map(str_strip_right)

    # Create dataframe with columns we made
    data_final_df = data_df[['Underlying', 'HappProd', 'X', 'CP', 'Price']]

    # Write to file
    data_final_df.to_csv(OUTPUT_FILE_PATH, sep='\t', encoding='utf-8',
                         index=False, header=False, quoting=QUOTE_NONE)

####################################################################################################
################## SCRIPT END ######################################################################
####################################################################################################

if __name__ == '__main__':
    run()
