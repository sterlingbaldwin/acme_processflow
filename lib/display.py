import curses
from datetime import datetime
from time import sleep
from lib.YearSet import SetStatus

def xy_check(x, y, hmax, wmax):
    if y >= hmax or x >= wmax:
        return -1
    else:
        return 0

def write_line(pad, line, x=None, y=None, color=None):
    if not color:
        color = curses.color_pair(4)
    try:
        pad.addstr(y, x, line, color)
    except:
        pass

def display(stdscr, event, job_sets):
    """
    Display current execution status via curses
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
            # Check if screen was re-sized (True or False)
            resize = curses.is_term_resized(height, width)

            # Action in loop if resize is True:
            if resize is True:
                height, width = stdscr.getmaxyx()
                hmax = height - 3
                wmax = width - 5 
                stdscr.clear()
                curses.resizeterm(height, width)
                stdscr.refresh()
                pad = curses.newpad(hmax, wmax)
                try:
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                except:
                    sleep(0.5)
                    continue
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
                        type=job.get_type(),
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
                        #deltastr = str(delta)
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

            x = 0
            if last_y:
                y = last_y
            pad.clrtobot()
            y += 1
            events = event_list.list
            for line in events[-10:]:
                if 'Transfer' in line.message:
                    continue
                if 'hosted' in line.message:
                    continue
                if 'failed' in line.message or 'FAILED' in line.message:
                    prefix = '[-]  '
                    try:
                        pad.addstr(y, x, prefix, curses.color_pair(4))
                    except:
                        continue
                else:
                    prefix = '[+]  '
                    try:
                        pad.addstr(y, x, prefix, curses.color_pair(5))
                    except:
                        continue
                try:
                    pad.addstr(y, x, line.message, curses.color_pair(4))
                except:
                    continue
                pad.clrtoeol()
                if initializing:
                    sleep(0.01)
                    pad.refresh(0, 0, 3, 5, hmax, wmax)
                y += 1
                if xy_check(x, y, hmax, wmax) == -1:
                    sleep(1)
                    break
            pad.clrtobot()
            y += 1
            if xy_check(x, y, hmax, wmax) == -1:
                sleep(1)
                continue

            file_start_y = y
            file_end_y = y
            file_display_list = []
            current_year = 1
            year_ready = True
            partial_data = False

            y = file_end_y + 1
            x = 0
            msg = 'Active transfers: {}'.format(active_transfers)
            try:
                pad.addstr(y, x, msg, curses.color_pair(4))
            except:
                pass
            pad.clrtoeol()
            if active_transfers:
                for line in events:
                    if 'Transfer' in line.message:
                        index = line.message.find('%')
                        if index:
                            s_index = line.message.rfind(' ', 0, index)
                            percent = float(line.message[s_index: index])
                            if percent < 100:
                                y += 1
                                try:
                                    pad.addstr(y, x, line.message, curses.color_pair(4))
                                except:
                                    pass
                                pad.clrtoeol()
            for line in events:
                if 'hosted' in line.message:
                    y += 1
                    try:
                        pad.addstr(y, x, line.message, curses.color_pair(4))
                    except:
                        pass
            spin_line = spinner[spin_index]
            spin_index += 1
            if spin_index == spin_len:
                spin_index = 0
            y += 1
            try:
                pad.addstr(y, x, spin_line, curses.color_pair(4))
            except:
                pass
            pad.clrtoeol()
            pad.clrtobot()
            y += 1
            if event and event.is_set():
                # enablePrint()
                return
            try:
                pad.refresh(0, 0, 3, 5, hmax, wmax)
            except:
                pass
            initializing = False
            sleep(1)

    except KeyboardInterrupt as e:
        raise

def sigwinch_handler(n, frame):
    curses.endwin()
    curses.initscr()

def start_display(event, job_sets):
    try:
        curses.wrapper(display, event, job_sets)
    except KeyboardInterrupt as e:
        return
