import os

from AwesomeProcessor import AwesomeProcessor
from RerieveFiles import RetrieveFiles

input_file_path = 'sc_reference.csv'
file_dir = os.path.dirname(os.path.abspath(__file__))
# retrieve = RetrieveFiles()
#
# for top, dirs, files in os.walk(file_dir):
#     for file_name in files:
#         if not file_name.endswith('sc_reference.csv'):
#             continue
#         input_file_path = os.path.join(file_dir, file_name)
#
#         retrieve.get_files()
awesome = AwesomeProcessor(os.path.join(file_dir, input_file_path))
awesome.start_processing()
