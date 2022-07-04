import os

from AwesomeProcessor import AwesomeProcessor
from RerieveFiles import RetrieveFiles

file_dir = os.path.dirname(os.path.abspath(__file__))
input_file_path = os.path.join(file_dir, 'sc_reference.csv')

retrieve = RetrieveFiles()
retrieve.get_files()
awesome = AwesomeProcessor(input_file_path)
awesome.start_processing()