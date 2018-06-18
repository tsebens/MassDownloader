from time import sleep
from typing import List

from case import Case
from conf import MAX_ALLOWABLE_ERR_COUNT
from exceptions import FileWriteError, DuplicateAgentError
from agent import AgentFactory, Agent
from status import StatusReport


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

    def administrate(self, s=0.1):
        """Contains all actions the CaseOfficer should perform during each update cycle"""
        self.fill_agent_roster()
        self.administrate_cases()
        sleep(s)

    def administrate_cases(self):
        """Check on the status of all active cases, and handle each as is appropriate"""
        for agent in self.active_agents:
            report = agent.process() # Retrieve the StatusReport
            self.administrate_case(agent, report)

    def administrate_case(self, agent: Agent, report: StatusReport):
        """Decide what to do with an Agent and its Case based on the StatusReport"""
        #TODO: Check - Is it possible for both of these to be true simultaneously?
        if agent.case.should_be_iced():
            self.ice_case(self.kill_agent(agent))
        if agent.case.should_be_closed():
            self.close_case(self.kill_agent(agent))
        '''If none of the above cases is true, then we should just let the agent continue.'''

    def fill_agent_roster(self):
        """Activate agents until the max number of Agents that can be active has been reached"""
        while len(self.active_agents) < self.max_active_agents:
            agent = self.sleeper_agents.pop()
            try:
                self.activate_agent(agent)
            except FileWriteError as fwe:
                """The file is taking too long to create. Restart the download"""
                # TODO: How should I handle a file write error?
                # Methinks the responsibility should be handed to the Agent. The agent can log the
                # incident, restart the download, and then if this happens too many times the case
                # will reflect the fact that it needs to be iced.
                agent.case.record.log_error(fwe)
                if agent.case.should_be_iced():
                    self.ice_case(agent.case)
                    self.kill_agent(agent)
                else:
                    # Effectively restarts the download. Not the prettiest
                    self.deactivate_agent(agent)

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