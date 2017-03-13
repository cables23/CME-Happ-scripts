# Standards
import sys
import os
import glob
import datetime
import time

# Locals
from tz_interface import tz_dropbox, tz_files
from tz_interface.tz_happ import tz_happ_auto, tz_happ_tables

# CONSTANTS SET BY SYS ARG #########################################################################

# only do a complete backup if told to
if 'complete' in sys.argv:
    SCRIPT_ARGS = (True, 16, 190)
else:
    SCRIPT_ARGS = (False, 12, 45)

# only send signal to processor if in production
if 'p' in sys.argv:

    import warnings
    warnings.simplefilter('ignore')

    SEND_SIGNAL = True
    DB_DUMP_DIR = 'dbDump_Complete'

else:

    import warnings
    warnings.simplefilter('ignore')

    SEND_SIGNAL = False
    DB_DUMP_DIR = 'dbDump_Complete_test'

# FUNCTIONS ########################################################################################


def wait_for_backup(wait_time):

    prefix = 'Waiting %d seconds to allow happ backup to process. Continuing in ' % wait_time
    digs = len(str(wait_time))

    for i in range(wait_time):

        print(prefix + str(wait_time - i).zfill(digs), end='\r')

        time.sleep(1)

    print(prefix + str(0).zfill(digs))


# FILE PATH STUFF ##################################################################################

DROPBOX_PATH = tz_dropbox.toStr()
HAPP_PARENT_BACKUP_DIR = os.path.join(DROPBOX_PATH, 'tenDb_raw', DB_DUMP_DIR)

# SCRIPT ###########################################################################################


def run(include_hist_in_dump, n_bins_expect, backup_time_allowance):

    print('Starting make_happ_backup')

    # The folder name, folder path, and zip where bin files will go
    happ_backup_dir_name = datetime.datetime.now().strftime('%Y_%m_%d_%H%M') + '_happDb'
    happ_backup_dir = os.path.join(HAPP_PARENT_BACKUP_DIR, happ_backup_dir_name)
    happ_backup_zip = happ_backup_dir + '.zip'

    # Running backup and waiting for it to finish
    tz_happ_auto.happ_backup_database(HAPP_PARENT_BACKUP_DIR, include_hist_in_dump)
    wait_for_backup(backup_time_allowance)

    # Gathering resulting .bin paths
    backup_bin_paths = glob.glob(os.path.join(HAPP_PARENT_BACKUP_DIR, '*.bin'))

    # Checking number of bin files created are what we expect
    n_bins_actual = len(backup_bin_paths)
    status_msg = '%d of %d bin files created by happ backup.' % (n_bins_actual, n_bins_expect)
    print({True: 'SUCCESS: %s', False: 'FAILURE: %s'}[n_bins_actual == n_bins_expect] % status_msg)

    # Zip the bin files
    tz_files.make_zip(backup_bin_paths, happ_backup_zip)
    print('Zip created: %s' % happ_backup_zip)

    # Making new folder
    tz_files.ensure_dir_exists(happ_backup_dir)

    # Moving .bin files into new folder
    tz_files.move_files(backup_bin_paths, happ_backup_dir)
    print('BINs put in: %s' % happ_backup_dir)

    # Sends signal that backup is complete
    if SEND_SIGNAL:
        tz_happ_tables.generate_file_signal()
        print('Signal sent.')

    print('COMPLETED: make_happ_backup')


if __name__ == '__main__':
    run(*SCRIPT_ARGS)
