from datetime import timedelta, datetime
from genericpath import isfile, getsize
from multiprocessing import Queue, Process
from os import remove
from queue import Empty
from time import sleep
from urllib.request import urlopen, urlcleanup, urlretrieve

from case import Case
from conf import STATUS_STAGING, STATUS_CREATING_FILE, STATUS_ACTIVE, STATUS_COMATOSE, STATUS_DEAD, STATUS_COMPLETE, \
    FILE_CREATION_TIMEOUT, REPORT_DONE, REPORT_WAIT, REPORT_KILL
from exceptions import StatusError, FileWriteError, MethodUnimplementedException
from status import StatusReport


class AgentFactory:
    """Factory object for Agent objects"""
    def agent(self, case: Case):
        return Agent(case) # Yeah, it's superfluous right now, but I get the feeling this will be useful later.


class Agent:
    # TODO: All datetime objects are currently naive. Could be a good idea to make a
    """Class which is reponsible for the complete transfer of a specified file from server to local disk"""
    def __init__(self, case: Case, tz=None):
        self.case = case # The local path where the file will be stored.
        self.status = STATUS_STAGING  # IMPORTANT: Only the agent should alter it's own status
        self.p = None
        self.last_size = 0
        self.dead_checks = 0
        self.restarts = 0
        self.DEAD_CHECK_THRESHOLD = 5 # The number of times the download can look dead before the process is restarted.
        self.DL_ATTEMPT_THRESHOLD = 5 # The number of times the download can be restarted before the entire download is abandoned.
        self.init_start_time = self.now()
        self.most_recent_start_time = None
        self.size_on_server = None
        self.tz=tz # A tzinfo object. Potentially unnecessary, but timezones can be SUCH a bitch, and this is SO easy...
        self.q = Queue()

    def process(self):
        """Manage the download case, and return a status report to the CaseOfficer"""\
        """In future iterations of this class, this function will be the function that is automatically and regularly 
        called, and therefore will be the function that needs to be defined by the developer."""\
        # Update the status record
        # TODO: Is there some way to make this more functional? Right now, it's functionality is completely side effects
        status = self.get_status()
        self.set_status(status)
        if status == STATUS_STAGING:
            raise StatusError("Agent is in active group but has STAGING status") # This shouldn't be possible
        if status == STATUS_ACTIVE:
            self.on_active() # Nothing to do if the process is still active.
        if status == STATUS_COMATOSE:
            self.on_comatose() # Also nothing to do if the process appears comatose.
        if status == STATUS_DEAD:
            self.on_dead()
        if status == STATUS_COMPLETE:
            self.on_complete() # These if cases are mostly here just in case I think of something to put here in the future.
        '''Check for errors. Log any that appear'''
        err = self.get_err()
        if err is not None:
            self.case.record.log_error(err=err)
        '''Create and return a StatusReport'''
        return self.status_report()

    def status_report(self):
        """Returns a StatusReport object, which is used by the CaseOfficer to determine the next course of action."""
        sod = self.get_size_on_disk()
        sos = self.get_size_on_server()
        curr_dl_run_time = self.curr_dl_run_time()
        speed = sod / curr_dl_run_time.total_seconds() # Speed in bytes/sec
        eta = self.get_eta(speed, sod, sos)
        total_run_time = self.get_total_runtime()
        '''Create and return the status report'''
        return StatusReport(self.make_report_status(self.get_status()),
                            speed, curr_dl_run_time, total_run_time, eta)

    def get_total_runtime(self):
        """Return the total running time of this agent as a timedelta object"""
        total_run_time = self.now() - self.init_start_time
        return total_run_time

    def get_eta(self, speed, sod, sos):
        """Return the estimated time of completion as a datetime obj"""
        eta_sec = (sos - sod) / speed  # ETA of dl completion in seconds
        eta = self.now() + timedelta(seconds=eta_sec)  # ETA as a datetime object
        return eta

    def make_report_status(self, status):
        """Based on the state of the download, return a report status for the CaseOfficer."""\
        '''All that the CO cares about is whether it should leave the agent along, retire it, or kill it.'''
        if status in (STATUS_STAGING, STATUS_CREATING_FILE, STATUS_ACTIVE, STATUS_COMATOSE):
            return REPORT_WAIT
        if status in (STATUS_COMPLETE,):
            return REPORT_DONE
        if status in (STATUS_DEAD,):
            return REPORT_KILL
        raise StatusError('Agent has unknown status: %s' % status)


    def get_status(self):
        """Check the state of the download, and return the new status accordingly"""\
        '''DOES NOT CHANGE THE STATUS ATTRIBUTE OF THE AGENT'''
        if self.status == STATUS_STAGING:
            return STATUS_STAGING
        else:
            if self.dl_is_alive():
                return STATUS_ACTIVE
            if self.dl_looks_dead():
                self.dead_checks += 1
                return STATUS_COMATOSE
            if self.dl_is_dead():
                return STATUS_DEAD
            if self.dl_complete():
                return STATUS_COMPLETE
        raise StatusError("Agent has unknown status.")

    def set_status(self, status):
        """A bit superfluous, but I think it's good practice"""
        self.status = status

    def wait_for_file_creation(self, timeout=FILE_CREATION_TIMEOUT):
        """Wait for the creation of the file, and returns when the file has been created. Throws an error on timeout"""
        start_time = self.now()
        while not isfile(self.case.fp):  # Wait for the file to get created, then register the agent as active
            sleep(0.2)
            time_passed = self.now() - start_time
            if time_passed.seconds > timeout:
                raise FileWriteError('Timeout: File still hasn\'t been created.')

    def curr_dl_run_time(self):
        """Return a timedelta object representative of the run time of the current download attempt"""
        return self.now() - self.most_recent_start_time

    def get_size_on_disk(self):
        """Get the current size of the file as it exists on the local disk"""
        # TODO: Stateful. Change to accept case as a parameter
        return getsize(self.case.fp)

    def get_size_on_server(self):
        """Get the size of the file on the server it's being downloaded from"""
        # TODO: Stateful. Change to accept case as a parameter
        if self.size_on_server is None:
            '''If the size on server hasn't been set, then we need to fetch the information'''
            d = urlopen(self.case.url)
            size = int(d.info()['Content-Length'])
            urlcleanup()
            self.size_on_server = size
        '''Once here, we know that the size has been determined.'''
        return self.size_on_server

    def get_err(self):
        """Check the queue for an error message sent by the download process. If there is none, return None"""
        if self.p is None:
            raise ReferenceError('Attempted to check queue, but process has not been started.')
        try:
            return self.q.get(timeout=1)
        except Empty:
            '''This exception means that the queue is empty'''
            return None

    def dl_start(self):
        """Spawns a process which will download the file to disk, and returns a reference to that process"""
        p = Process(target=dl_file, args=(self.case, self.q))
        p.start()
        dl_file(self.case, self.q)
        self.most_recent_start_time = self.now()
        self.wait_for_file_creation()
        self.status = STATUS_ACTIVE
        self.p = p

    def dl_kill(self):
        """Terminate process. Will throw NoneType Error if the process has not been started."""
        self.p.terminate()
        self.p.join()

    def dl_cleanup(self):
        """Deletes the file."""
        remove(self.case.fp)

    def dl_restart(self):
        """Kills the current process, and begins a new download process."""
        self.dl_kill()
        self.dl_cleanup()
        self.dl_start()

    def dl_complete(self):
        """Returns true if the file on disk is the same size as it's counterpart on the server"""
        size_on_server = self.get_size_on_server()
        size_on_disk = self.get_size_on_disk()
        if size_on_disk >= size_on_server:
            return True
        return False

    def dl_looks_dead(self):
        """Returns true if the download LOOKS like it has died. Doesn't necessarily mean that it IS dead"""
        size = self.get_size_on_disk()
        if size == self.last_size:
            return True
        return False

    def dl_is_dead(self):
        """Returns true if the download is showing every indication of being dead."""
        if self.dl_looks_dead() and self.dead_checks > self.DEAD_CHECK_THRESHOLD:
            return True
        return False

    def dl_is_alive(self):
        """Returns true if the download appears to still be alive"""
        size = self.get_size_on_disk()
        if size > self.last_size:
            self.last_size = size
            self.status = STATUS_ACTIVE
            return True
        if size < self.last_size:
            # raise FileWriteWarning('File has shrunk since last check.\n fp: %s' % self.case.fp)
            print('File has shrunk')
        return False

    def dl_check(self):
        """Returns true if the size of the file on disk matches the size of the file on server"""
        if self.dl_complete():
            return True
        return False

    def dissolve(self):
        """Cleanup function, if there is any that needs to be done. Should be called after the download is complete"""
        raise MethodUnimplementedException('Agent.dissolve has not yet been implemented.')

    def now(self):
        """Return a datetime object of the current moment."""
        #TODO: This should be timezone sensitive. Just because
        return datetime.now()


    '''
    Placeholder methods. One gets called every time the case is administrated. Which one depends
    on the download status
    '''
    def on_active(self):
        pass

    def on_comatose(self):
        pass

    def on_dead(self):
        self.dl_restart()
        self.dead_checks = 0
        self.restarts += 1

    def on_complete(self):
        self.case.record.complete = True

class ExceptionPackage:
    """A package object which a download process can write to the queue for the Agent to read."""
    def __init__(self, exc: Exception):
        """We have to record all of the relevant information as primitive types, because otherwise,
        they can't be serialized."""
        self.args = list(exc.args)


def dl_file(case, q):
    """Basic file download function. Attempts to download the given url to the given file path"""
    try:
        urlretrieve(case.url, case.fp)
        urlcleanup()
    except Exception as exc:
        '''If an error get's raised, put it into the queue.'''
        q.put(exc.args)