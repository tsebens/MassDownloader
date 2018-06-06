import urllib
from os import remove
from os.path import getsize, join, basename, isfile
from multiprocessing import Process

from datetime import datetime
from time import sleep
from urllib.request import urlretrieve, urlcleanup, urlopen

from exceptions import MethodUnimplementedException, FileWriteError, StatusError, DuplicateAgentError, FileWriteWarning

STATUS_STAGING = 1
STATUS_ACTIVE = 2
STATUS_COMATOSE = 3
STATUS_DEAD = 4
STATUS_COMPLETE = 5


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

    def args(self):
        return {'url': self.url, 'fp': self.fp}


class AgentFactory:
    """Factory object for Agent objects"""
    def agent(self, case: Case):
        return Agent(case) # Yeah, it's superfluous right now, but I get the feeling this will be useful later.


class Agent:
    """Class which is reponsible for the complete transfer of a specified file from server to local disk"""
    def __init__(self, case: Case):
        self.case = case # The local path where the file will be stored.
        self.status = STATUS_STAGING # IMPORTANT: Only the agent should alter it's own status
        self.p = None
        self.last_size = 0
        self.dead_checks = 0
        self.restarts = 0
        self.DEAD_CHECK_THRESHOLD = 5 # The number of times the download can look dead before the process is restarted.
        self.DL_ATTEMPT_THRESHOLD = 5 # The number of times the download can be restarted before the entire download is abandoned.

    def size_on_disk(self):
        """Get the current size of the file as it exists on the local disk"""
        return getsize(self.case.fp)

    def size_on_server(self):
        """Get the size of the file on the server it's being downloaded from"""
        d = urlopen(self.case.url)
        size = int(d.info()['Content-Length'])
        urlcleanup()
        return size

    def update_status(self):
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


    def dl_file(self):
        """Basic file download function. Attempts to download the given url to the given file path"""
        try:
            urlretrieve(self.case.url, self.case.fp)
            urlcleanup()
        except:
            

    def dl_start(self):
        """Spawns a process which will download the file to disk, and returns a reference to that process"""
        p = Process(target=self.dl_file)
        p.start()
        self.wait_for_file_creation()
        self.status = STATUS_ACTIVE
        self.p = p

    def wait_for_file_creation(self):
        """Wait's for the creation of the file, and returns when the file has been created. Throws an error on timeout"""
        start_time = datetime.now()
        while not isfile(self.case.fp):  # Wait for the file to get created, then register the agent as active
            sleep(0.2)
            time_passed = datetime.now() - start_time
            if time_passed.seconds > 30:
                raise FileWriteError('Timeout: File still hasn\'t been created')


    def dl_stop(self):
        """Terminate the process. Will throw a NoneType Error if the process has not been started. Deletes the downloaded file"""
        self.p.terminate()
        self.p.join()
        # Delete the file. Stopping the process means that the download was interrupted.
        # We can't retrieve it, and the file is incomplete. Keeping the file on disk would be misleading.
        remove(self.case.fp)

    def dl_restart(self):
        """Kills the current process, and begins a new download process."""
        self.dl_stop()
        self.dl_start()

    def dl_complete(self):
        """Returns true if the file on disk is the same size as it's counterpart on the server"""
        size_on_server = self.size_on_server()
        size_on_disk = self.size_on_disk()
        if size_on_disk >= size_on_server:
            return True
        return False

    def dl_looks_dead(self):
        """Returns true if the download LOOKS like it has died. Doesn't necessarily mean that it IS dead"""
        size = self.size_on_disk()
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
        size = self.size_on_disk()
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
        if self.dl_looks_dead():
            self.dead_checks += 1
        if self.dead_checks > self.DEAD_CHECK_THRESHOLD:
            self.dl_restart()
            self.dead_checks = 0
            self.restarts += 1
        return False

    def dissolve(self):
        """Cleanup function, if there is any that needs to be done. Should be called after the download is complete"""
        raise MethodUnimplementedException('Agent.dissolve has not yet been implemented.')


class CaseOfficer:
    """Class for managing multiple Agent instances. Spawns them as necessary, stops them if asked, and checks on their progress."""
    def __init__(self, max_active_agents=1):
        self.active_agents = []
        self.sleeper_agents = []
        self.retired_agents = []
        self.max_active_agents = max_active_agents
        self.agent_factory = AgentFactory()

    def add_cases(self, cases):
        """Accept a list of args, and then register a list of Agents to this CaseOfficer, one for each argument pair"""
        self.register_agents(self.assign_agents(cases))

    def active_cases(self):
        """Returns true if there are still open Cases"""
        if len(self.active_agents) > 0 or len(self.sleeper_agents) > 0:
            return True
        return False

    def administrate(self):
        """Contains all actions the CaseOfficer should perform during each update cycle"""
        self.fill_agent_roster()
        self.administrate_cases()

    def administrate_cases(self):
        """Check on the status of all active cases, and handle each as is appropriate"""
        for agent in self.active_agents:
            self.administrate_case(agent)

    def administrate_case(self, agent: Agent):
        """Manage the download case."""
        # TODO: Is there some way to make this more functional? Right now, it's functionality is completely side effects
        status = agent.update_status()
        if status == STATUS_STAGING:
            raise StatusError("Agent is in active group but has STAGING status")
        if status == STATUS_ACTIVE:
            pass # Nothing to do if the agent is still active.
        if status == STATUS_COMATOSE:
            pass # Also nothing to do if the agent appears comatose.
        if status == STATUS_DEAD:
            agent.dl_restart()
        if status == STATUS_COMPLETE:
            self.retire_agent(agent)

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
        return self.sleeper_agents + self.active_agents + self.retired_agents

    def activate_agent(self, agent: Agent):
        """Begins the download assigned to the passed Agent"""
        self.active_agents.append(agent)
        agent.dl_start()

    def activate_agents(self, agents):
        """Activates all passed Agents"""
        for agent in agents:
            self.activate_agent(agent)

    def retire_agent(self, agent: Agent):
        """Mark a download case as closed. The file has been retrieved, checked, and validated."""
        self.active_agents.remove(agent)
        self.retired_agents.append(agent)






