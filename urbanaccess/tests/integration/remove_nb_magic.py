import sys
import argparse
import os

file_to_convert_path = os.path.join(os.path.dirname(os.path.realpath(
    __file__)), "../../..", 'demo')


def remove_magic_lines(inputFileName, outputFileName,
                       path=file_to_convert_path):
    print('Looking in path {}: '.format(file_to_convert_path))
    input = open(os.path.join(path, inputFileName), "r")
    output = open(os.path.join(path, outputFileName), "w")

    output.write(input.readline())

    for line in input:
        if not line.lstrip().startswith('get_ipython()'):
            output.write(line)

    input.close()
    output.close()

    print('NB magic lines removed from .py file')


if __name__ == '__main__':

    if len(sys.argv) > 1:
        parser = argparse.ArgumentParser()
        parser.add_argument("-in", "--inputFileName", type=str,
                            help="input .py filename")
        parser.add_argument("-out", "--outputFileName", type=str,
                            help="output .py filename")

        args = parser.parse_args()
        inputFileName = args.inputFileName
        outputFileName = args.outputFileName

        remove_magic_lines(inputFileName, outputFileName)

    else:

        print('No arguments passed.')
