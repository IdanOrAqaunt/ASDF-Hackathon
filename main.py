import os

from AwesomeProcessor import AwesomeProcessor

file_dir = os.path.dirname(os.path.abspath(__file__))
input_file_path = os.path.join(file_dir, 'sc_reference.csv')
awesome = AwesomeProcessor(input_file_path)
awesome.start_processing()