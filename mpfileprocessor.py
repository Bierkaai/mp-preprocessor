import os

__author__ = 'coen'

import time
import logging
import multiprocessing

from Queue import Full, Empty


class LoggingProcess(multiprocessing.Process):
    def __init__(self, name="unknown"):
        super(LoggingProcess, self).__init__()
        self.logger = multiprocessing.get_logger()
        self.name = name

    def debug(self, m):
        self.log(logging.DEBUG, m)

    def info(self, m):
        self.log(logging.INFO, m)

    def warning(self, m):
        self.log(logging.WARNING, m)

    def error(self, m):
        self.log(logging.ERROR, m)

    def log(self, level, m):
        self.logger.log(level, "{0}: {1}".format(self.name, m))


class QueueHandler(object):
    def __init__(self, q, timeout=5, retries=5):
        self.q = q
        self.timeout = timeout
        self.retries = retries

    def put(self, obj):
        retries = 0
        success = False
        while not success and not retries >= self.retries:
            try:
                self.q.put(obj, True, self.timeout)
                success = True
            except Full:
                retries += 1
        if success:
            return retries
        raise Full

    def get(self):
        retries = 0
        success = False
        while not success and not retries >= self.retries:
            try:
                obj = self.q.get(True, self.timeout)
                success = True
            except Empty:
                retries += 1
        if success:
            return obj
        raise Empty


class FileProcessor(LoggingProcess):
    def __init__(self, filename, queue, **kwargs):
        assert (isinstance(filename, str))
        if 'name' in kwargs:
            super(FileProcessor, self).__init__(kwargs['name'])
        else:
            super(FileProcessor, self).__init__()
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        else:
            timeout = 5
        if 'retries' in kwargs:
            retries = kwargs['retries']
        else:
            retries = 5
        if "outputpernlines" in kwargs:
            self.outputpernlines = kwargs["outputpernlines"]
        else:
            self.outputpernlines = 0
        self.q = QueueHandler(queue, timeout, retries)
        self.filename = filename
        self.debug("Fileprocessor {0} initiated for file {1}"
                   .format(self.name, self.filename))


class FileReader(FileProcessor):
    """ Reads a file to a queue
    """

    def __init__(self, filename, queue, **kwargs):
        """

        :param filename: filename to read from
        :param queue: queue to write to
        :param kwargs: name boolean, skipfirstline boolean, countlines ['no', 'yes', 'estimate'], outputpernlines int (0=never)
        :return:
        """
        super(FileReader, self).__init__(filename, queue, **kwargs)
        if "skipfirstline" in kwargs:
            self.skipfirstline = kwargs["skipfirstline"]
        else:
            self.skipfirstline = False
        if "countlines" in kwargs:
            self.countlines = kwargs["countlines"]
        else:
            self.countlines = 'no'


    def run(self):
        self.info("Forked, starting main reading function.")
        self.readLines()
        self.info("Exited main reading function. Waiting to join.")

    def getLineCount(self):
        # TODO: look into python fadvice for POSIX

        count = -1

        # Count lines
        if self.countlines == 'yes':
            self.info("Counting lines in file {0}"
                      .format(self.filename))
            with open(self.filename, 'r') as c_obj:
                for i, l in c_obj:
                    pass
                if self.skipfirstline:
                    count = i
                else:
                    count = i + 1
            self.info("Counted {0} lines in {1}"
                      .format(count, self.filename))

        # Estimate no of lines by file size
        elif self.countlines == 'estimate':
            self.info("Estimating number of lines in file {0}"
                      .format(self.filename))
            with open(self.filename, 'r') as c_obj:
                if self.skipfirstline:
                    c_obj.readline()
                for i in range(4):
                    c_obj.readline()
                linesize_estimate = float(c_obj.tell()) / 4
            filesize = os.stat(self.filename).st_size
            count = int(round(
                float(filesize) /
                (float(linesize_estimate))))
            self.info("Estimated {0} lines in {1} " +
                      "({2} bytes, estimated {3} bytes/line)"
                      .format(count,
                              self.filename,
                              filesize,
                              linesize_estimate))

        return count

    def readLines(self):
        countstart = time.time()
        count = self.getLineCount()
        counttime = time.time() - countstart
        self.info("Getting line count took {0} seconds.".format(counttime))

        with open(self.filename, 'r') as f_obj:
            start = time.time()
            est_block_start = time.time()
            est_block_read = 0
            if self.skipfirstline:
                line1 = f_obj.next()
                if len(line1) > 20:
                    line1 = line1[:20]
                self.info("Skipped first line of file: {0}..."
                          .format(line1))

            for i, line in enumerate(f_obj):
                if i % self.outputpernlines == 0:
                    read = i + 1
                    time_pass = float(time.time() - start)
                    speed = float(read) / time_pass
                    cur_speed = (read - float(est_block_read)) / \
                                (float(time.time() / est_block_start))
                    if count > 0:
                        est_remaining = (count - read) * cur_speed
                        self.info("Read {0} of {1} lines in {2} seconds, " +
                                  "avg. {3} lines/sec, cur. {4} lines/sec" +
                                  "approx. {5} seconds remaining."
                                  .format(read, count,
                                          time_pass, speed, cur_speed,
                                          est_remaining))
                    else:
                        self.info("Read {0} lines in {1} seconds. " +
                                  "avg. {2} lines/sec, cur. {3} lines/sec"
                                  .format(read, time_pass, speed, cur_speed))
                    est_block_start = time.time()
                    est_block_read = read
                try:
                    self.q.put(line)
                except Full:
                    self.error("Queue is full after {0} retries. Timeout: {1} seconds."
                               .format(self.q.retries, self.q.timeout))
        read = i + 1
        time_pass = float(time.time() - start)
        speed = float(read) / time_pass
        self.info("Finished reading file, wrote {0} lines to queue " +
                  "in {1} seconds, {2} lines/sec."
                  , format(read, time_pass, speed))


class FileWriter(FileProcessor):
    def __init__(self, filename, queue, **kwargs):
        super(FileWriter, self).__init__(filename, queue, **kwargs)

    def run(self):
        with open(filename, 'a') as f_obj:
            start = time.time()
            qempty = False
            written = 0

            while not qempty:
                try:
                    line = self.q.get()
                    if line[-1] == '\n':
                        f_obj.write(line)
                    else:
                        f_obj.write(line + '\n')
                    if written % self.outputpernlines == 0:
                        self.outputSpeedStats(written, start)

                except Empty:
                    self.info("Queue is empty after {0} retries"
                              .format(self.q.retries))
                    self.qempty = True
            f_obj.flush()
            os.fsync()
        self.outputSpeedStats(written, start)
        self.info("Exiting, wrote entire queue to {0}"
                  .format(self.filename))

    def outputSpeedStats(self, written, start):
        written += 1
        time_pass = float(time.time() - start)
        speed = float(written) / time_pass
        self.info("Wrote {0} lines in {1} seconds. " +
                  "{2} lines/sec"
                  .format(written, time_pass, speed))


