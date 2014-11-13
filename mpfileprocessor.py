import csv
import os

__author__ = 'coen'

import time
import logging
import logging.handlers
import multiprocessing

from Queue import Full, Empty

get_logger = multiprocessing.get_logger


def configure_logger(logfile='mpfileprocessor.log', loglevel=logging.DEBUG):
    logger = get_logger()

    logger.setLevel(loglevel)
    handler = logging.handlers.RotatingFileHandler(
        logfile, maxBytes=20000000, backupCount=100)
    formatter = logging.Formatter('%(asctime)s-%(levelname)s-[%(processName)s-%(process)d] - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.debug("Logger configured using module function")
    return logger


class LoggingProcess(multiprocessing.Process):
    def __init__(self, name=None):
        super(LoggingProcess, self).__init__()
        self.logger = multiprocessing.get_logger()
        if name is None:
            self.name = self.__class__.__name__
        else:
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
        self.logger.log(level, m)


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
        obj = None
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


class BaseLineProcessor(LoggingProcess):
    def __init__(self, in_q, out_q, **kwargs):
        if 'name' in kwargs:
            super(BaseLineProcessor, self).__init__(kwargs['name'])
        else:
            super(BaseLineProcessor, self).__init__()
        self.in_q = QueueHandler(in_q)
        self.out_q = QueueHandler(out_q)
        self.debug("Processor initiated...")

    def run(self):
        qempty = False
        self.debug("Starting main loop")
        while not qempty:
            try:
                line = self.in_q.get()
            except Empty:
                self.info("Queue empty, exiting...")
                qempty = True
            else:
                processed = self.process_line(line)
                try:
                    self.out_q.put(processed)
                except Full:
                    self.error("Output queue is full! exiting, DATA LOSS!")
                    qempty = True
                finally:
                    self.in_q.q.task_done()
        self.finalactions()

    def process_line(self, line):
        """
        Should be overridden in subclasses
        :param line: input line as str
        :return: processed line as str
        """
        return line

    def finalactions(self):
        pass


class FunctionLineProcessor(BaseLineProcessor):
    def __init__(self, in_q, out_q, function, **kwargs):
        super(FunctionLineProcessor, self).__init__(
            in_q, out_q, **kwargs
        )
        self.processLine = function


class QueueSplitter(LoggingProcess):
    def __init__(self, in_q, out1_q, out2_q, **kwargs):
        if 'name' in kwargs:
            super(QueueSplitter, self).__init__(kwargs['name'])
        else:
            super(QueueSplitter, self).__init__()
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        else:
            timeout = 5
        if 'retries' in kwargs:
            retries = kwargs['retries']
        else:
            retries = 5
        self.in_q = QueueHandler(in_q, timeout, retries)
        self.out1_q = QueueHandler(out1_q, timeout, retries)
        self.out2_q = QueueHandler(out2_q, timeout, retries)

    def run(self):
        qempty = False
        self.debug("Starting main loop")
        while not qempty:
            try:
                line = self.in_q.get()
            except Empty:
                self.info("Queue empty, exiting...")
                qempty = True
            else:
                try:
                    self.out1_q.put(line)
                    self.out2_q.put(line)
                except Full:
                    self.error("Either output queue is full. DATA LOSS! Exiting...")
                    qempty = True
                finally:
                    self.in_q.q.task_done()
        self.debug("Main loop exited...")


class FileReader(FileProcessor):
    """ Reads a file to a queue
    """

    def __init__(self, filename, queue, **kwargs):
        """

        :param filename: filename to read from
        :param queue: queue to write to
        :param kwargs: name boolean, skipfirstline boolean, countlines boolean, outputpernlines int (0=never)
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
            self.countlines = False
        self.count = 0
        self.est_block_read = 0
        self.est_block_start = 0
        self.readstarttime = 0

    def run(self):
        self.info("Forked, starting main reading function.")
        self.readlines()
        self.info("Exited main reading function. Waiting to join.")

    def getLineCount(self):
        # TODO: look into python fadvice for POSIX
        countstart = time.time()

        # Count lines
        if self.countlines:
            self.info("Counting lines in file {0}"
                      .format(self.filename))
            with open(self.filename, 'r') as c_obj:
                for i, l in enumerate(c_obj):
                    pass
                if self.skipfirstline:
                    count = i
                else:
                    count = i + 1
            self.info("Counted {0} lines in {1}"
                      .format(count, self.filename))

        # Estimate no of lines by file size
        else:
            self.info("Estimating number of lines in file {0}"
                      .format(self.filename))
            with open(self.filename, 'r') as c_obj:
                if self.skipfirstline:
                    c_obj.readline()
                lines = 0
                go = True
                while lines < 100 and go:
                    l = c_obj.readline()
                    lines += 1
                    if l == "":
                        go = False
                bytes = c_obj.tell()
            linesize_estimate = float(bytes) / lines
            self.info("Read first {0} lines = {1} bytes."
                      .format(lines, bytes))
            filesize = os.stat(self.filename).st_size
            count = int(round(
                float(filesize) /
                (float(linesize_estimate))))
            self.info("Estimated {0} lines in {1} ({2} bytes, estimated {3} bytes/line)"
                      .format(count,
                              self.filename,
                              filesize,
                              linesize_estimate))

        self.count = count
        counttime = time.time() - countstart
        self.info("Getting line count took {0} seconds.".format(counttime))


    def outputupdate(self, i):
        if i % self.outputpernlines == 0:
            read = i + 1
            time_pass = float(time.time() - self.readstarttime)
            speed = float(read) / time_pass
            cur_speed = float(read - float(self.est_block_read)) / \
                        (float(time.time() / self.est_block_start))
            if self.count > 0:
                est_remaining = (self.count - read) / cur_speed
                self.info(
                    "Read {0} of {1} lines in {2} seconds, avg. {3} lines/sec, "
                    .format(read, self.count,
                            time_pass, speed) +
                    "cur. {0} lines/sec approx. {1} seconds remaining."
                    .format(cur_speed, est_remaining))
            else:
                self.info("Read {0} lines in {1} seconds. avg. {2} lines/sec, cur. {3} lines/sec"
                          .format(read, time_pass, speed, cur_speed))
            self.est_block_start = time.time()
            self.est_block_read = read

    def outputfinalstatus(self, i):
        read = i + 1
        time_pass = float(time.time() - self.readstarttime)
        speed = float(read) / time_pass
        self.info("Finished reading file, wrote {0} lines to queue in {1} seconds, {2} lines/sec."
                  .format(read, time_pass, speed))


    def starttimers(self):
        self.readstarttime = time.time()
        self.est_block_start = time.time()
        self.est_block_read = 0

    def putlineonqueue(self, line):
        try:
            self.q.put(line)
        except Full:
            self.error("Queue is full after {0} retries. Timeout: {1} seconds."
                       .format(self.q.retries, self.q.timeout))

    def readlines(self):
        self.getLineCount()
        with open(self.filename, 'r') as f_obj:
            self.starttimers()

            if self.skipfirstline:
                line1 = f_obj.next()
                if len(line1) > 20:
                    line1 = line1[:20]
                self.info("Skipped first line of file: {0}..."
                          .format(line1))

            for i, line in enumerate(f_obj):
                self.outputupdate(i)
                self.putlineonqueue(line)

            self.outputfinalstatus(i)
            self.q.q.close()


class CSVFileReader(FileReader):
    def readlines(self):
        self.getLineCount()
        with open(self.filename, 'rb') as f_obj:
            self.starttimers()

            if self.skipfirstline:
                line1 = f_obj.next()
                if len(line1) > 20:
                    line1 = line1[20:]
                self.info("Skipped first line of file {0}..."
                          .format(line1))
            # TODO: make the csv setting a parameter
            reader = csv.reader(f_obj,
                                quotechar='"',
                                quoting=csv.QUOTE_ALL,
                                delimiter=';')
            for i, line in enumerate(reader):
                self.outputupdate(i)
                self.putlineonqueue(line)

            self.outputfinalstatus(i)
            self.q.q.close()


class FileWriter(FileProcessor):
    def __init__(self, filename, queue, **kwargs):
        """

        :param filename:
        :param queue:
        :param kwargs: overwrite (boolean, default=False)
        :return:
        """
        super(FileWriter, self).__init__(filename, queue, **kwargs)
        self.written = 0
        self.qempty = False
        self.starttime = 0
        if 'overwrite' in kwargs:
            self.overwrite = kwargs['overwrite']
        else:
            self.overwrite = False

    def run(self):
        self.info("Forked, starting main writing function.")
        self.writelines()
        self.info("Exited main writing function. Waiting to join.")

    def settimer(self):
        self.starttime = time.time()

    def writelines(self):
        with open(self.filename, 'w') as f_obj:
            self.settimer()

            while not self.qempty:
                try:
                    line = self.q.get()
                    if line[-1] == '\n':
                        f_obj.write(line)
                    else:
                        f_obj.write(line + '\n')
                    self.q.q.task_done()
                    self.written += 1
                    self.speedstatsinloop()

                except Empty:
                    self.info("Queue is empty after {0} retries"
                              .format(self.q.retries))
                    qempty = True
            self.forcefilesync(f_obj)

        self.outputspeedstats()
        self.info("Exiting, wrote entire queue to {0}"
                  .format(self.filename))

    def forcefilesync(self, f_obj):
        f_obj.flush()
        os.fsync(f_obj)

    def speedstatsinloop(self):
        if self.written % self.outputpernlines == 0:
            self.outputspeedstats()

    def outputspeedstats(self):
        time_pass = float(time.time() - self.starttime)
        speed = float(self.written) / time_pass
        self.info("Wrote {0} lines in {1} seconds. {2} lines/sec"
                  .format(self.written, time_pass, speed))


class CSVFileWriter(FileWriter):
    def writelines(self):
        with open(self.filename, 'w') as f_obj:
            self.settimer()

            # TODO make csv dialect configurable though parameters
            writer = csv.writer(f_obj,
                                quotechar='"',
                                quoting=csv.QUOTE_ALL,
                                delimiter=';')

            while not self.qempty:
                try:
                    l = self.preprocess(self.q.get())
                    assert (isinstance(l, list))
                    writer.writerow(l)
                    self.q.q.task_done()
                    self.written += 1
                    self.speedstatsinloop()
                except Empty:
                    self.info("Queue is empty after {0} retries"
                              .format(self.q.retries))
                    self.qempty = True

            self.forcefilesync(f_obj)
        self.outputspeedstats()
        self.info("Exiting, wrote entire queue to {0}"
                  .format(self.filename))

    def preprocess(self, line):
        return line


class DictToCSVFileWriter(CSVFileWriter):
    def __init__(self, filename, queue, fields, **kwargs):
        super(DictToCSVFileWriter, self).__init__(filename, queue, **kwargs)
        self.fields = fields

    def createList(self, dictionary):
        rlist = []
        for fieldname in self.fields:
            try:
                rlist.append(dictionary[fieldname])
            except KeyError:
                rlist.append(None)
        return rlist


    def preprocess(self, line):
        assert (isinstance(line, dict))
        return self.createList(line)
