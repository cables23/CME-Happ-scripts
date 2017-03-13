# Standards
import os

import pandas as pd
# pd.set_option('max_rows', 500)

from datetime import datetime as dt

# Locals
from tz_interface import tz_dropbox
from tz_interface.tz_happ import tz_happ_tables

from tz_calculation import tz_bls

DROPBOX_PATH = tz_dropbox.toStr()

####### SCRIPT ##################################################################################

def run():

    # pulling the altest cleaned month table
    m_df = tz_happ_tables.get_last_cleaned_db_dic()['month']

    # filtering for eurodollars
    m_df = m_df[m_df['u_abr'].str[0:2] == 'ED']

    def calc_ed_strikes(df_row):

        u_prc = df_row['skew_price']
        m_vol = df_row['slope_vatm'] / 100

        df_row['t_exp'] = (dt.strptime(df_row['dexp'], '%Y-%m-%d') - dt.today()).days / 365.25
        m_exp = df_row['t_exp']

        # common stuff
        df_row['prc'] = 0.0025
        df_row['acct'] = 'SQ'
        df_row['blank'] = ''

        df_row_p = df_row.copy()
        df_row_c = df_row.copy()

        # different stuff
        df_row_p['q'] = 1000
        df_row_c['q'] = -1000

        df_row_p['pc'] = 'P'
        df_row_c['pc'] = 'C'

        df_row_p['x'] = tz_bls.eu_opt_strike_ED(u_prc, -0.25, m_vol, m_exp, R=None, Q=None)[0]
        df_row_c['x'] = tz_bls.eu_opt_strike_ED(u_prc,  0.25, m_vol, m_exp, R=None, Q=None)[0]

        return pd.concat([df_row_p, df_row_c], axis=1)

    sub_df_list = [calc_ed_strikes(df_row) for index, df_row in m_df.iterrows()]

    df_out = pd.concat(sub_df_list, axis=1).T.reset_index(drop=True)

    # For QA purposes this is a pretty good view of the DF
    # df_out = df_out[['u_abr', 'm_abr', 'skew_price', 'slope_vatm', 't_exp', 'q', 'pc', 'x']].sort_values(['t_exp', 'u_abr']).reset_index(drop=True)

    # For output
    df_out = df_out[['q', 'm_abr', 'u_abr', 'x', 'pc', 'prc', 'blank', 'acct']].reset_index(drop=True)

    print(df_out)

    df_out.to_csv(os.path.join(DROPBOX_PATH, 'squash_input.csv'), index=False, header=False)

if __name__ == '__main__':
    run()