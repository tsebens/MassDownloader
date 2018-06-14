STATUS_STAGING = 1
STATUS_ACTIVE = 2
STATUS_COMATOSE = 3
STATUS_DEAD = 4
STATUS_COMPLETE = 5
FILE_CREATION_TIMEOUT = 30 # Seconds the application will wait for a file to be created before throwing a FileWriteError
MAX_ALLOWABLE_ERR_COUNT = 5 # Number of errors (of the same type) that can be thrown before a case is shelved