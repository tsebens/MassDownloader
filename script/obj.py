from os import remove
from os.path import getsize, join, basename, isfile
from multiprocessing import Process, Queue

from datetime import datetime, timedelta
from queue import Empty
from time import sleep
from typing import List
from urllib.request import urlretrieve, urlcleanup, urlopen

from conf import STATUS_STAGING, STATUS_ACTIVE, STATUS_COMATOSE, STATUS_DEAD, STATUS_COMPLETE, FILE_CREATION_TIMEOUT, \
    MAX_ALLOWABLE_ERR_COUNT
from exceptions import MethodUnimplementedException, StatusError, DuplicateAgentError, FileWriteError


def dl_file(case, q):
    """Basic file download function. Attempts to download the given url to the given file path"""
    try:
        urlretrieve(case.url, case.fp)
        urlcleanup()
    except Exception as exc:
        '''If an error get's raised, put it into the queue.'''
        q.put(exc)


class CaseRecord:
    """A class for recordiing the history for a particular Case. Useful in dealing with problem Cases"""
    def __init__(self):
        self.errs = {}

    def log_error(self, err: Exception):
        """Record the passed exception"""
        if err in self.errs:
            self.errs[err] += 1
        else:
            self.errs[err] = 1

    def total_err_count(self):
        """Return the total number of errors this Case has experienced"""
        count = 0
        for err_type in self.errs:
            count += self.errs[err_type]
        return count

    def max_err_count(self):
        """Return the type of error that has been thrown the most, along with the number of times it has been thrown"""
        '''If no errors have been thrown, returns (None, 0)'''
        if len(self.errs) == 0:
            '''No errors have been thrown yet'''
            return 0
        max_num = -1
        for err_type in self.errs:
            if self.errs[err_type] > max_num:
                max_num = self.errs[err_type]
        return max_num

    def max_err_type(self):
        """Return the type of error that has been thrown the most, along with the number of times it has been thrown"""
        '''If no errors have been thrown, returns (None, 0)'''
        if len(self.errs) == 0:
            '''No errors have been thrown yet'''
            return None, 0
        max_num = -1
        for err_type in self.errs:
            if self.errs[err_type] > max_num:
                max_num = self.errs[err_type]
                err_type = err_type
        return err_type


class StatusReport:
    """Object that the agent will pass back to the case officer to indicate the general heath of the download."""
    def __init__(self, status, speed, curr_dl_run_time, total_run_time, eta, record: CaseRecord):
        self.status = status
        self.speed = speed
        self.curr_dl_run_time = curr_dl_run_time
        self.total_run_time = total_run_time
        self.eta = eta
        self.record = record

    def err(self):
        """If the process has errored out, then return the error that has been thrown. Otherwise, return false."""
        if self.err is not None:
            return self.err
        return False

    def is_healthy(self):
        """If the download has not stopped for an unexpected reason, return true."""
        """Intended as a superficial check that can be done quickly to rule out necessary intervention"""
        if self.status in (STATUS_STAGING, STATUS_ACTIVE, STATUS_COMPLETE):
            return True
        return False

    def is_dead(self):
        """If the download is dead and needs to be restarted, return true."""
        if self.status == STATUS_DEAD or self.err:
            return True
        return False

    def is_done(self):
        """If the download has been completed, then return true"""
        if self.status == STATUS_COMPLETE:
            return True


class CaseFactory:
    """Factory object for Case objects"""
    def __init__(self, default_directory=None):
        self.default_directory=default_directory

    def case(self, url, fp='DEFAULT'):
        if fp == 'DEFAULT':
            fp = self.default_file_path(self.get_url_file_name(url))
        return Case(url, fp)

    def get_url_file_name(self, url):
        return basename(url)

    def default_file_path(self, file_name):
        if self.default_directory is None:
            raise AttributeError('CaseFactory has no default directory set, but one is required.')
        return join(self.default_directory, file_name)


class Case:
    def __init__(self, url, fp):
        self.url = url
        self.fp = fp
        self.record = CaseRecord()

    def args(self):
        return {'url': self.url, 'fp': self.fp}


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

    def administrate_case(self):
        """Manage the download case, and return a status report to the CaseOfficer"""
        '''Update the status record'''
        # TODO: Is there some way to make this more functional? Right now, it's functionality is completely side effects
        status = self.update_status()
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
        total_run_time = self.now() - self.init_start_time
        speed = sod / curr_dl_run_time.total_seconds() # Speed in bytes/sec
        eta_sec = (sos - sod) / speed # ETA of dl completion in seconds
        eta = self.now() + timedelta(seconds=eta_sec) # ETA as a datetime object
        return StatusReport(self.update_status(), speed, curr_dl_run_time, total_run_time, eta, self.case.record)

    def update_status(self):
        """Check the state of the download, and record the status accordingly"""
        if self.status != STATUS_STAGING:
            if self.dl_is_alive():
                self.status = STATUS_ACTIVE
            if self.dl_looks_dead():
                self.dead_checks += 1
                self.status = STATUS_COMATOSE
            if self.dl_is_dead():
                self.status = STATUS_DEAD
            if self.dl_complete():
                self.status = STATUS_COMPLETE
        return self.status

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
        pass


class CaseOfficer:
    """Class for managing multiple Agent instances. Spawns them as necessary, stops them if asked, and checks on their progress."""
    def __init__(self, max_active_agents=1):
        self.active_agents = []
        self.sleeper_agents = []
        self.closed_cases = []
        self.cold_cases = []
        self.max_active_agents = max_active_agents
        self.agent_factory = AgentFactory()

    def add_cases(self, cases: List):
        """Accept a list of args, and then register a list of Agents to this CaseOfficer, one for each argument pair"""
        self.register_agents(self.assign_agents(cases))

    def ice_case(self, case: Case):
        """Store a case that has proven too problematic"""
        self.cold_cases.append(case)

    def close_case(self, case: Case):
        """Case complete."""
        self.closed_cases.append(case)

    def cases_active(self):
        """Returns true if there are still open Cases"""
        if len(self.active_agents) > 0 or len(self.sleeper_agents) > 0:
            return True
        return False

    def administrate(self, s=5):
        """Contains all actions the CaseOfficer should perform during each update cycle"""
        self.fill_agent_roster()
        self.administrate_cases()
        sleep(s)

    def administrate_cases(self):
        """Check on the status of all active cases, and handle each as is appropriate"""
        for agent in self.active_agents:
            report = agent.administrate_case() # Retrieve the StatusReport
            self.handle_report(agent, report)

    def handle_report(self, agent: Agent, report: StatusReport):
        """Decide what to do with an Agent and its Case based on the StatusReport"""
        if self.case_should_be_iced(agent, report):
            self.ice_case(self.kill_agent(agent))
        if self.case_should_be_closed(agent, report):
            self.close_case(self.kill_agent(agent))

    def case_should_be_closed(self, agent: Agent, report:StatusReport):
        """Return true if the download has finished and the case should be closed."""
        return report.is_done()

    def case_should_be_shelved(self, agent: Agent, report:StatusReport):
        """Return true if the case should be shelved for later"""
        # TODO: Implement this.
        # Case should be shelved if the case is getting a lot of 'connection forcibly closed' errors
        return False

    def case_should_be_iced(self, agent: Agent, report:StatusReport):
        """Return true if, based on the Agent and the Case, the Case should be shelved"""
        record = agent.case.record
        if record.max_err_count() > MAX_ALLOWABLE_ERR_COUNT:
            return True
        return False

    def fill_agent_roster(self):
        """Activate agents until the max number of Agents that can be active has been reached"""
        while len(self.active_agents) < self.max_active_agents:
            self.activate_agent(self.sleeper_agents.pop())

    def register_agents(self, agents):
        """Add the passed agents to this CaseOfficer's list of sleeper agents"""
        self.sleeper_agents.extend(agents)

    def assign_agents(self, cases):
        """Create Agent object for each argument pair, and return them as a list"""
        for case in cases:
            yield self.assign_agent(case)

    def assign_agent(self, case: Case):
        """Create and return an agent assigned to the passed arguments"""
        return self.agent_factory.agent(case)

    def agent_assigned(self, url, fp):
        """Returns true if an agent already exists that is assigned to the passed argument pair"""
        # TODO: This is pretty inefficient. Rework this into a more cycle-sensitive process. A hash of some kind.
        for agent in self.all_agents():
            if agent.url == url and agent.fp == fp:
                raise DuplicateAgentError('Agent has already been assigned to Case' )
        return False

    def all_agents(self):
        """Returns all Agents registered to this CaseOfficer"""
        return self.sleeper_agents + self.active_agents

    def activate_agent(self, agent: Agent):
        """Begins the download assigned to the passed Agent"""
        self.active_agents.append(agent)
        agent.dl_start()

    def activate_agents(self, agents: List):
        """Activates all passed Agents"""
        for agent in agents:
            self.activate_agent(agent)

    def deactivate_agent(self, agent):
        """Deactivate active Agent, and return him to the pool of sleeper Agents"""
        agent.dl_kill()
        self.active_agents.remove(agent)
        self.sleeper_agents.append(agent)

    def kill_agent(self, agent: Agent):
        """Deactivate the passed agent, remove from the active_agent roster, and return that Agent's Case"""
        case = agent.case
        agent.dl_kill()
        self.active_agents.remove(agent)
        del agent
        return case

    def kill_agents(self, agents: List):
        """Deactivate a list of agents"""
        for agent in agents:
            yield self.kill_agent(agent)







