import curses
from datetime import datetime
from time import sleep
from lib.YearSet import SetStatus, SetStatusMap
from jobs.JobStatus import JobStatus, ReverseMap
from lib.util import strfdelta, print_debug


def handle_resize(pad, height, width, stdscr):
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
    else:
        hmax = height - 3
        wmax = width - 5

    return pad, hmax, wmax

def print_str(pad, line, color, y, x, hmax, wmax):
    if y >= (hmax - 10):
        x += 50
        y = 0
    try:
        pad.addstr(y, x, line, color)
    except Exception as e:
        print_debug(e)
        pass
    finally:
        return y, x

def print_job_info(pad, job, y, x, hmax, wmax, now):
    line = '  >   {type} -- {id} '.format(
        type=job.type,
        id=job.job_id)

    y, x = print_str(pad, line, curses.color_pair(4), y, x, hmax, wmax)
    tempx = x + len(line)
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
    if job.status == JobStatus.RUNNING \
            and job.start_time is not None:
        delta = now - job.start_time
        deltastr = strfdelta(delta, "{H}:{M}:{S}")
        line = '{status} run time: {time}'.format(
            status=ReverseMap[job.status],
            time=deltastr)
    # if job has ended, print total time
    elif job.status in [JobStatus.COMPLETED, JobStatus.FAILED] \
            and job.end_time is not None \
            and job.start_time is not None:
        delta = job.end_time - job.start_time
        line = '{status} elapsed time: {time}'.format(
            status=ReverseMap[job.status],
            time=strfdelta(delta, "{H}:{M}:{S}"))
    else:
        line = '{status}'.format(status=ReverseMap[job.status])

    y, _ = print_str(pad, line, color_pair, y, tempx, hmax, wmax)
    y += 1
    return y, x

def print_year_set_info(pad, year_set, y, x, hmax, wmax):
    if y + len(year_set.jobs) >= (hmax - 10):
        x += 50
        y = 0
    line1 = 'Year_set {num}: {start} - {end}: '.format(
        num=year_set.set_number,
        start=year_set.set_start_year,
        end=year_set.set_end_year)
    y, x = print_str(pad, line1, curses.color_pair(4), y, x, hmax, wmax)
    x += len(line1)

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
    line2 = '{status}'.format(
        status=SetStatusMap[year_set.status])
    y, x = print_str(pad, line2, color_pair, y, x, hmax, wmax)
    y += 1
    x -= len(line1)
    return y, x

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
        hmax, wmax = 0, 0
        while True:
            if event and event.is_set():
                return
            # Check if screen was re-sized (True or False)
            pad, hmax, wmax = handle_resize(pad, height, width, stdscr)
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
                y, x = print_year_set_info(pad, year_set, y, x, hmax, wmax)
                # # if the job_set is done collapse it
                if year_set.status == SetStatus.COMPLETED \
                        or year_set.status == SetStatus.NO_DATA \
                        or year_set.status == SetStatus.PARTIAL_DATA:
                    continue
                for job in year_set.jobs:
                    y, x = print_job_info(pad, job, y, x, hmax, wmax, now)


            # Transfer status
            if filemanager.active_transfers:
                msg = 'Active transfers: {}'.format(filemanager.active_transfers)
                try:
                    y, x = print_str(pad, msg, curses.color_pair(4), y, x, hmax, wmax)
                    # pad.addstr(y, x, msg, curses.color_pair(4))
                    # pad.clrtoeol()
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
                    # pad.clrtoeol()

            # Event log
            y += 1
            msg = "---- Status Log ----"
            y, x = print_str(pad, msg, curses.color_pair(4), y, x, hmax, wmax)
            y += 1
            events = event_list.list
            event_length = len(events) - 1
            for line in events[-10:]:
                if line == events[0]:
                    continue
                if 'Transfer in progress' in line.message:
                    continue
                if 'hosted' in line.message:
                    continue
                try:
                    msg = '{time}: {msg}'.format(
                        time=line.time.strftime('%I:%M:%S'),
                        msg=line.message)
                    pad.addstr(y, x, msg, curses.color_pair(4))
                    y += 1
                except:
                    continue

            y = hmax - 5
            x = 0
            # print current state
            color_pair = curses.color_pair(4)
            line = ">>> {}".format(events[0].message)
            pad.addstr(y, x, line, color_pair)
            y += 1

            # fidget spinner
            spin_line = spinner[spin_index]
            spin_index += 1
            if spin_index == spin_len:
                spin_index = 0
            try:
                pad.addstr(y, x, spin_line, curses.color_pair(4))
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
