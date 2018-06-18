# The various states that the download can be in
STATUS_STAGING = 0
STATUS_CREATING_FILE = 1
STATUS_ACTIVE = 2
STATUS_COMATOSE = 3
STATUS_DEAD = 4
STATUS_COMPLETE = 5

REPORT_WAIT = 0
REPORT_DONE = 1
REPORT_KILL = 3

FILE_CREATION_TIMEOUT = 30 # Seconds the application will wait for a file to be created before throwing a FileWriteError
MAX_ALLOWABLE_ERR_COUNT = 5 # Number of errors (of the same type) that can be thrown before a case is shelved