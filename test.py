__author__ = 'coen'

import mpfileprocessor
from multiprocessing import JoinableQueue


def info(m):
    print m
    mpfileprocessor.get_logger().info(m)


def duplicate(line):
    return line + "COPY: " + line

if __name__ == "__main__":
    mplogger = mpfileprocessor.configure_logger()
    mplogger.debug('TEST')

    readerqueue = JoinableQueue()
    writerqueue = JoinableQueue()

    reader = mpfileprocessor.FileReader(
        'in.dat', readerqueue, outputpernlines=1, name='HappyFileReader')
    writer = mpfileprocessor.FileWriter(
        'out.dat', writerqueue, outputpernlines=1, overwrite=True,
        timeout=1, retries=2)
    processor1 = mpfileprocessor.LineProcessor(
        readerqueue, writerqueue, duplicate, name='LineDuplicator1',
        timeout=1, retries=2
    )
    processor2 = mpfileprocessor.LineProcessor(
        readerqueue, writerqueue, duplicate, name='LineDuplicator2',
        timeout=1, retries=2
    )

    info("Starting reader")
    reader.start()
    info("Starting writer")
    writer.start()
    info("Starting 2 processors")
    processor1.start()
    processor2.start()
    info("Waiting for reader to join")
    reader.join()
    info("Waiting for readerqueue to join")
    readerqueue.join()
    info("Waiting for processors to join")
    processor1.join()
    processor2.join()
    info("Waiting for writerqueue to join")
    writerqueue.join()
    info("Waiting for writer to join")
    writer.join()
    info("All done, thank you for your patience")





