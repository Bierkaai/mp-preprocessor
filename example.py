import sys

__author__ = 'coen'



from multiprocessing import JoinableQueue
from mpfileprocessor import FileReader, FileWriter, FunctionLineProcessor


def remove_first_character(line):
    if len(line) > 1:
        return line[1:]
    return line



if __name__ == "__main__":
    inputfile = sys.argv[1]
    outputfile = sys.argv[2]
    
    reader_queue = JoinableQueue()
    writer_queue = JoinableQueue()

    reader = FileReader(inputfile, reader_queue)
    processor = FunctionLineProcessor(reader_queue, writer_queue, remove_first_character)
    writer = FileWriter(outputfile, writer_queue)

    processor.start()
    reader.start()
    writer.start()


