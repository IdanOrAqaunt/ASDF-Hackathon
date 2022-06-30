import numpy as np
import pandas as pd
import io


def start_processing(input_csv: io.StringIO):
    df = pd.read_csv(input_csv)