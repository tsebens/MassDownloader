from os.path import basename, join

from conf import MAX_ALLOWABLE_ERR_COUNT


class CaseRecord:
    """A class for recordiing the history for a particular Case. Useful in dealing with problem Cases"""
    def __init__(self):
        self.errs = {}
        self.complete = False

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
            return None
        max_num = -1
        max_type = None
        for err_type in self.errs:
            if self.errs[err_type] > max_num:
                max_num = self.errs[err_type]
                max_type = err_type
        return max_type


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

    def should_be_closed(self):
        """Return true if the download has finished and the case should be closed."""
        return self.record.complete

    def should_be_shelved(self):
        """Return true if the case should be shelved for later"""
        # TODO: Implement this.
        # Case should be shelved if the case is getting a lot of 'connection forcibly closed' errors
        return False

    def should_be_iced(self, max_count=MAX_ALLOWABLE_ERR_COUNT):
        """Return true if the Case should be iced"""
        if self.record.max_err_count() > max_count:
            return True
        return False