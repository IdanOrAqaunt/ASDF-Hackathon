import numpy as np
import pandas as pd
import io

class AwesomeProcessor:

    def start_processing(self, input_csv: io.StringIO):
        df = pd.read_csv(input_csv)
        #column delta dates
        cool_df = pd.DataFrame()
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        df['original_install_date'] = pd.to_datetime(df['original_install_date'])
        cool_df['installation_to_event_delta'] = df['visit_date'] - df['original_install_date']


        #colums count parts replaced

        cool_df['total_relaced_per_asset'] = df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
        cool_df['customer_name'] = df['customer_name']
        cool_df['total_relaced_per_asset'] = df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
        cool_df['total_relaced_per_customer'] = df.groupby(['customer_name'])['part_id'].transform('nunique')
        cool_df['part_total_replaced_per_customer'] = df.groupby(['customer_name'])['part_id'].transform('nunique')
        cool_df['total_part_per_name'] = df.groupby(['part_id'])['part_id'].transform('nunique')
        cool_df['total_part_per_name'] = df['part_id'].value_counts()
        cool_df['total_part_cost'] = df['total_part_cost']
        self.normelize(cool_df, 'total_part_per_name', 'total_part_cost', )
        #column part importency by price

        #total cost per visit
        #normelize all columns

        #what exactly is the input the train the model
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
        df1.loc[df1["template version"] <= 0, 'cust_normalised_overall_score'] = 0
        return df1['cust_normalised_overall_score']