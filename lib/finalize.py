import os
import logging
import time

from lib.mailer import Mailer
from lib.util import print_message, print_line, print_debug


def finalize(config, event_list, status, kill_event, runmanager):
    if status == 1 and config['global'].get('native_grid_cleanup') in [1, '1', 'true', 'True']:
        message = 'Performing post run cleanup'
        native_cleanup(config)
    else:
        message = 'Leaving native grid files in place'
    print_message(message, 'ok')

    message = 'All processing complete' if status == 1 else "One or more job failed"
    code = 'ok' if status == 1 else 'error'
    print_message(message, code)
    emailaddr = config['global'].get('email')
    if emailaddr:
        message='Sending notification email to {}'.format(emailaddr)
        print_message(message, 'ok')
        try:
            if status == 1:
                msg = 'Your processflow run has completed successfully\n'
                status = msg
            else:
                msg = 'One or more processflow jobs failed\n'
                status = msg
                msg += 'See log for additional details\n{}\n'.format(config['global']['log_path'])

            for case in runmanager.cases:
                msg += '==' + '='*len(case['case']) + '==\n'
                msg += ' # ' + case['case'] + ' #\n'
                msg += '==' + '='*len(case['case']) + '==\n\n'
                for job in case['jobs']:
                    msg += '\t > ' + job.get_report_string() + '\n'
                msg += '\n'

            m = Mailer(src='processflowbot@llnl.gov', dst=emailaddr)
            m.send(
                status=status,
                msg=msg)
        except Exception as e:
            print_debug(e)

    logging.info("All processes complete")

def native_cleanup(config):
    """
    Remove non-regridded output files after processflow completion
    """
    for case in config['simulations']:
        if case in ['start_year', 'end_year', 'comparisons']: continue
        native_path = os.path.join(
            config['global']['project_path'],
            'output', 'pp',
            config['simulations'][case]['native_grid_name'])
        if os.path.exists(native_path):
            try:
                rmtree(native_path)
            except OSError:
                return False
    
    return True
