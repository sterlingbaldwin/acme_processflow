import curses
from datetime import datetime
from time import sleep
from lib.YearSet import SetStatus
from jobs.JobStatus import JobStatus
from lib.util import strfdelta


def handle_resize(pad, height, width):
    """
    Checks for and handles a window resize event

    Parameters:
        pad (curses pad): the pad to check
        height (int): The height of the stdscr
        width (int): the width of the stdscr
    Returns:
        pad: either the new pad if the window was resized, or the original pad
    """
    resize = curses.is_term_resized(height, width)
    if resize is True:
        height, width = stdscr.getmaxyx()
        hmax = height - 3
        wmax = width - 5
        stdscr.clear()
        curses.resizeterm(height, width)
        stdscr.refresh()
        pad = curses.newpad(hmax, wmax)
        pad.refresh(0, 0, 3, 5, hmax, wmax)

    return pad


def display(stdscr, event, job_sets, filemanager, event_list):
    """
    Display current execution status via curses

    Parameters:
        stdscr (curses screen): the main screen object
        event (thread.event): an event to turn the GUI on and off
        job_sets (list): the list of jobsets from the RunManager
        filemanager (FileManager): The main threads FileManager
        event_list (EventList): the main threads EventList

    """

    # setup variables
    initializing = True
    height, width = stdscr.getmaxyx()
    hmax = height - 3
    wmax = width - 5
    spinner = ['\\', '|', '/', '-']
    spin_index = 0
    spin_len = 4
    try:
        # setup curses
        stdscr.nodelay(True)
        curses.curs_set(0)
        curses.init_pair(1, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(2, curses.COLOR_GREEN, curses.COLOR_WHITE)
        curses.init_pair(3, curses.COLOR_RED, curses.COLOR_BLACK)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_BLACK)
        curses.init_pair(5, curses.COLOR_GREEN, curses.COLOR_BLACK)
        curses.init_pair(6, curses.COLOR_MAGENTA, curses.COLOR_BLACK)
        curses.init_pair(7, curses.COLOR_YELLOW, curses.COLOR_BLACK)
        curses.init_pair(8, curses.COLOR_BLACK, curses.COLOR_BLACK)
        stdscr.bkgd(curses.color_pair(8))

        pad = curses.newpad(hmax, wmax)
        last_y = 0
        while True:
            if event and event.is_set():
                return
            # Check if screen was re-sized (True or False)
            pad = handle_resize(pad, height, width)
            now = datetime.now()
            # sleep until there are jobs
            if len(job_sets) == 0:
                sleep(1)
                continue
            pad.refresh(0, 0, 3, 5, hmax, wmax)
            pad.clear()
            y = 0
            x = 0
            for year_set in job_sets:
                line = 'Year_set {num}: {start} - {end}'.format(
                    num=year_set.set_number,
                    start=year_set.set_start_year,
                    end=year_set.set_end_year)
                try:
                    pad.addstr(y, x, line, curses.color_pair(1))
                except:
                    continue
                pad.clrtoeol()
                y += 1

                color_pair = curses.color_pair(4)
                if year_set.status == SetStatus.COMPLETED:
                    # set color to green
                    color_pair = curses.color_pair(5)
                elif year_set.status == SetStatus.FAILED:
                    # set color to red
                    color_pair = curses.color_pair(3)
                elif year_set.status == SetStatus.RUNNING:
                    # set color to purple
                    color_pair = curses.color_pair(6)
                line = 'status: {status}'.format(
                    status=year_set.status)
                try:
                    pad.addstr(y, x, line, color_pair)
                except:
                    continue
                if initializing:
                    sleep(0.01)
                    try:
                        pad.refresh(0, 0, 3, 5, hmax, wmax)
                    except:
                        continue
                pad.clrtoeol()
                y += 1

                # if the job_set is done collapse it
                if year_set.status == SetStatus.COMPLETED \
                        or year_set.status == SetStatus.NO_DATA \
                        or year_set.status == SetStatus.PARTIAL_DATA:
                    continue
                for job in year_set.jobs:
                    line = '  >   {type} -- {id} '.format(
                        type=job.type,
                        id=job.job_id)
                    try:
                        pad.addstr(y, x, line, curses.color_pair(4))
                    except:
                        continue
                    color_pair = curses.color_pair(4)
                    if job.status == JobStatus.COMPLETED:
                        color_pair = curses.color_pair(5)
                    elif job.status in [JobStatus.FAILED, 'CANCELED', JobStatus.INVALID]:
                        color_pair = curses.color_pair(3)
                    elif job.status == JobStatus.RUNNING:
                        color_pair = curses.color_pair(6)
                    elif job.status == JobStatus.SUBMITTED or job.status == JobStatus.PENDING:
                        color_pair = curses.color_pair(7)
                    # if the job is running, print elapsed time
                    if job.status == JobStatus.RUNNING:
                        delta = now - job.start_time
                        deltastr = strfdelta(delta, "{H}:{M}:{S}")
                        line = '{status} elapsed time: {time}'.format(
                            status=job.status,
                            time=deltastr)
                    # if job has ended, print total time
                    elif job.status in [JobStatus.COMPLETED, JobStatus.FAILED] \
                            and job.end_time \
                            and job.start_time:
                        delta = job.end_time - job.start_time
                        line = '{status} elapsed time: {time}'.format(
                            status=job.status,
                            time=strfdelta(delta, "{H}:{M}:{S}"))
                    else:
                        line = '{status}'.format(status=job.status)
                    try:
                        pad.addstr(line, color_pair)
                    except:
                        continue
                    pad.clrtoeol()
                    if initializing:
                        sleep(0.01)
                        pad.refresh(0, 0, 3, 5, hmax, wmax)
                    y += 1

            # print current state
            events = event_list.list
            color_pair = curses.color_pair(4)
            line = ">>> {}".format(events[0].message)
            try:
                y += 1
                pad.addstr(y, x, line, color_pair)
                y += 1
            except:
                continue
            pad.clrtobot()
            y += 1

            # Transfer status
            if filemanager.active_transfers:
                msg = 'Active transfers: {}'.format(filemanager.active_transfers)
                try:
                    pad.addstr(y, x, msg, curses.color_pair(4))
                    pad.clrtoeol()
                except:
                    pass

                for line in events:
                    if 'Transfer' not in line.message:
                        continue
                    index = line.message.find('%')
                    if index == -1:
                        continue
                    s_index = line.message.rfind(' ', 0, index)
                    percent = float(line.message[s_index: index])
                    msg = line.message
                    if percent >= 100:
                        continue
                    try:
                        pad.addstr(y, x, msg, curses.color_pair(4))
                        y += 1
                    except:
                        pass
                    pad.clrtoeol()

            # Event log
            event_length = len(events) - 1
            for index in range(10):
                i = event_length - index
                if i == 0:
                    break
                line = events[i]
                if 'Transfer' in line.message:
                    continue
                if 'hosted' in line.message:
                    continue
                if 'failed' in line.message or 'FAILED' in line.message:
                    prefix = '[-]  '
                    color_pair = curses.color_pair(4)
                else:
                    prefix = '[+]  '
                    color_pair = curses.color_pair(5)
                try:
                    pad.addstr(y, x, prefix, curses.color_pair(4))
                except:
                    continue
                try:
                    msg = '{time}: {msg}'.format(
                        time=line.time.strftime('%I:%M:%S'),
                        msg=line.message)
                    pad.addstr(y, x, msg, curses.color_pair(4))
                    y += 1
                except:
                    continue
                pad.clrtoeol()
                if initializing:
                    sleep(0.01)
                    pad.refresh(0, 0, 3, 5, hmax, wmax)

            # fidget spinner
            spin_line = spinner[spin_index]
            spin_index += 1
            if spin_index == spin_len:
                spin_index = 0
            try:
                pad.addstr(y, x, spin_line, curses.color_pair(4))
            except:
                pass

            # print cycle clean up
            try:
                pad.clrtoeol()
                pad.clrtobot()
                pad.refresh(0, 0, 3, 5, hmax, wmax)
            except:
                pass
            initializing = False
            sleep(1)

    except KeyboardInterrupt as e:
        return


def start_display(event, job_sets, filemanager, event_list):
    try:
        curses.wrapper(display, event, job_sets, filemanager, event_list)
    except KeyboardInterrupt as e:
        return
