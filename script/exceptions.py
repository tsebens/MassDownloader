class DuplicateAgentError(Exception):
    pass


class MethodUnimplementedException(Exception):
    """Exception to indicate that an unimplemented method has been called"""
    pass


class FileWriteError(Exception):
    """Indicates that something has gone wrong with the writing of a file"""
    pass


class FileWriteWarning(Warning):
    """Indicates some non-fatal file writing weirdness"""
    pass


class StatusError(Exception):
    """Indicates an unexpected or impossible Agent status"""
    pass