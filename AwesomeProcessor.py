import os

import numpy as np
import pandas as pd
import io

class AwesomeProcessor:

    def __init__(self, input_csv: io.StringIO):
        self.__input_path =input_csv

    def start_processing(self):
        df = pd.read_csv(self.__input_path)

        cool_df = df.copy()
        try:
            cool_df['visit_date'] = pd.to_datetime(df['visit_date'])
            cool_df['original_install_date'] = pd.to_datetime(df['original_install_date'])
            cool_df['visit_date'] = pd.to_datetime(df['visit_date'])
            cool_df['installation_to_event_delta'] = cool_df['visit_date'] - cool_df['original_install_date']


            #colums count parts replaced

            cool_df['total_relaced_per_asset'] = cool_df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
            cool_df['customer_name'] = cool_df['customer_name']
            cool_df['total_relaced_per_asset'] = cool_df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
            cool_df['total_relaced_per_customer'] = cool_df.groupby(['customer_name'])['part_id'].transform('nunique')
            cool_df['part_total_replaced_per_customer'] = cool_df.groupby(['customer_name'])['part_id'].transform('nunique')
            cool_df['total_part_per_name'] = cool_df.groupby(['part_name', 'producttype_name'])['part_name'].transform('count')

            # column part importency by price
            cool_df['part_importency'] = self.normelize(cool_df, 'total_part_per_name', 'part_cost', 5, 10)

            #print the csv
            cool_df(os.path.dirname(os.path.abspath(__file__))  + '/' + 'awesome.csv')
        #total cost per visit
        #normelize all columns
        #what exactly is the input the train the model

        except Exception as e:
            print("exception: ", e)




    def normelize(self, df: pd.DataFrame, s_from: str, s_to: str, weight_from:int, weight_to:int):
        df1 = df.copy()
        df1['normelized_s_from'] = ((df1[s_from] - df1[s_from].min()) / (
                    df1[s_from].max() - df1[s_from].min())) + 1
        df1['normelized_s_to'] = ((df1[s_to] - df1[s_to].min()) / (
                    df1[s_to].max() - df1[s_to].min())) + 1

        max_potential_score = 2 * (weight_from + weight_to)
        min_potential_score = 1 * (weight_from + weight_to)
        df1['cast overall score'] = (weight_from * df1['normelized_s_from']) + (
                    weight_to * df1['normelized_s_to'])
        df1['cust_normalised_overall_score'] = (df1['cast overall score'] - min_potential_score) / (
                    max_potential_score - min_potential_score) * 100
        return df1['cust_normalised_overall_score']