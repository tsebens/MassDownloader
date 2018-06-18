from datetime import datetime

from case import CaseRecord
from conf import STATUS_STAGING, STATUS_ACTIVE, STATUS_COMPLETE, STATUS_DEAD


class StatusReport:
    """Object that the agent will pass back to the case officer to indicate the general heath of the download."""
    def __init__(self, status, speed, curr_dl_run_time, total_run_time, eta, timestamp=datetime.now()):
        self.status = status
        self.speed = speed
        self.curr_dl_run_time = curr_dl_run_time
        self.total_run_time = total_run_time
        self.eta = eta
        self.timestamp = timestamp

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