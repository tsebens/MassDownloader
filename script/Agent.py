import urllib
from os.path import getsize
from multiprocessing import Process

from os import remove
from typing import List

STATUS_STAGING = 1
STATUS_ACTIVE = 2
STATUS_DEAD = 3
STATUS_COMPLETE = 4



class DuplicateAgentError(Exception):
    pass

class MethodUnimplementedException(Exception):
    """Exception to indicate that an unimplemented method has been called"""
    pass


class Agent:
    """Class which is reponsible for the complete transfer of a specified file from server to local disk"""
    def __init__(self, url, fp):
        self.url = url # The URL that points to the file on server
        self.fp = fp   # The local path where the file will be stored.
        self.status = STATUS_STAGING
        self.p = None
        self.last_size = 0
        self.dead_checks = 0
        self.restarts = 0
        self.DEAD_CHECK_THRESHOLD = 5 # The number of times the download can look dead before the process is restarted.
        self.DL_ATTEMPT_THRESHOLD = 5 # The number of times the download can be restarted before the entire download is abandoned.

    def size_on_disk(self):
        """Get the current size of the file as it exists on the local disk"""
        return getsize(self.fp)

    def size_on_server(self):
        """Get the size of the file on the server it's being downloaded from"""
        d = urllib.urlopen(self.url)
        size = int(d.info()['Content-Length'])
        urllib.urlcleanup()
        return size

    def dl_file(self):
        """Basic donwnload file. Attempts to download the given url to the given file path"""
        urllib.urlretrieve(self.url, self.fp)
        urllib.urlcleanup()
        return True

    def dl_start(self):
        """Spawns a process which will download the file to disk, and returns a reference to that process"""
        p = Process(target=self.dl_file, args=(self.url, self.fp))
        p.start()
        self.status = STATUS_ACTIVE
        self.p = p

    def dl_stop(self):
        """Terminate the process. Will throw a NoneType Error if the process has not been started. Deletes the downloaded file"""
        self.p.terminate()
        # Delete the file. Stopping the process means that the download was interrupted.
        # We can't retrieve it, and the file is incomplete. Keeping the file on disk would be misleading.
        remove(self.fp)

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
        """Returns true if the download LOOKS like it has died. Doesn't necessarily mean that it is dead"""
        size = self.size_on_disk()
        if size > self.last_size:
            return True
        return False

    def dl_check(self):
        """Returns true if the download has completed, and the file on disk matches the size of the file on server"""
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


class AgentFactory:
    """Factory object for Agent objects"""
    def agent(self, url, fp):
        return Agent(url, fp) # Yeah, it's superfluous right now, but I get the feeling this will be useful later.


class CaseOfficer:
    """Class for managing multiple Agent instances. Spawns them as necessary, stops them if asked, and checks on their progress."""
    def __init__(self):
        self.active_agents = []
        self.sleeper_agents = []
        self.args = {}
        self.agent_factory = AgentFactory()

    def assign_agents(self, args):
        """Create Agent object for each argument pair, and return them as a list"""
        for arg in args:
             self.assign_agent(arg)

    def assign_agent(self, arg):
        url = arg['url']
        fp = arg['fp']
        if self.agent_assigned(url, fp):
            raise DuplicateAgentError(
                'An agent has already been assigned to this argument pair\nu: %s - fp: %s' % (url, fp)
            )
        return self.agent_factory.agent(url, fp)

    def agent_assigned(self, url, fp):
        """Returns true if an agent already exists that is assigned to the passed argument pair"""
        # TODO: Implement this
        return False
