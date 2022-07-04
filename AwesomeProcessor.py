from datetime import datetime
import os

import numpy as np
import pandas as pd
import io

class AwesomeProcessor:

    def __init__(self, input_csv: io.StringIO):
        self.__input_path =input_csv

    '''
    general trend
    assume df has warranty_end, cost_new_asset, expected_lifespan
    '''
    def age_cost(self, df: pd.DataFrame):
        df['age'] =\
            pd.to_datetime(df['event_date']) - pd.to_datetime(df['original_install_date'])

        df['period_age'] = round(df['age'].dt.days / 30)
        df['period_age'] = df['period_age'].astype(int)
        df['cases'] = df.groupby(['period_age'])['period_age'].transform(lambda x: x.count())
        return df.groupby(['asset_id', 'productype_name', 'period_age']).agg(
            cost_sum = pd.NamedAgg(column='total_cost', aggfunc='sum'),
            cases = pd.NamedAgg(column='cases', aggfunc='count'),
            warranty_end = pd.NamedAgg(column='warranty_end', aggfunc='first'),
            expected_lifespan = pd.NamedAgg(column='expected_lifespan', aggfunc='first'),
            cost_new_asset = pd.NamedAgg(column='cost_new_asset', aggfunc='first')
        )


    def start_processing(self):
        df = pd.read_csv(self.__input_path)

        try:
            # cool_df['visit_date'] = pd.to_datetime(df['visit_date'])
            # cool_df['original_install_date'] = pd.to_datetime(df['original_install_date'])
            # cool_df['visit_date'] = pd.to_datetime(df['visit_date'])
            # cool_df['installation_to_event_delta'] = cool_df['visit_date'] - cool_df['original_install_date']
            #
            #
            # #colums count parts replaced
            #
            # cool_df['total_relaced_per_asset'] = cool_df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
            # cool_df['customer_name'] = cool_df['customer_name']
            # cool_df['total_relaced_per_asset'] = cool_df.groupby(['asset_id', 'part_id'])['part_id'].transform('nunique')
            # cool_df['total_relaced_per_customer'] = cool_df.groupby(['customer_name'])['part_id'].transform('nunique')
            # cool_df['part_total_replaced_per_customer'] = cool_df.groupby(['customer_name'])['part_id'].transform('nunique')
            # cool_df['total_part_per_name'] = cool_df.groupby(['part_name', 'producttype_name'])['part_name'].transform('count')
            #
            # # column part importency by price
            # cool_df['part_importency'] = self.normelize(cool_df, 'total_part_per_name', 'part_cost', 10, 5)
            #
            # cool_df['time_b_visits'] = cool_df.sort_values(['visit_date'], ascending=False).groupby(['asset_id'])[
            #     'installation_to_event_delta'].diff(-1)
            # q = cool_df['total_part_per_name'].quantile(.005)
            # cool_df = cool_df[cool_df['total_part_per_name'] > q]
            # df_event_cum = cool_df.copy()
            # df_event_cum = df_event_cum.groupby('investigation_id', as_index=False).agg(
            #     {'part_cost': 'sum', 'labor_cost': 'max', 'event_date': 'max'}).sort_values('event_date')
            # df_event_cum['total_cost'] = df_event_cum['part_cost'] + df_event_cum['labor_cost']

            small_data = df[df.model_name == 'GROUP-GROUP CYCLE'][
                ['investigation_id', 'producttype_name', 'model_name', 'asset_id', 'field_agent_name', 'part_id',
                 'event_category', 'event_date', 'part_unit_cost', 'part_cost', 'labor_cost', 'original_install_date', 'part_name']]
            small_data['total_part_per_name'] = small_data.groupby(['part_name', 'producttype_name'])['part_name'].transform('count')
            q = small_data['total_part_per_name'].quantile(.1)
            list_of_asset_out = small_data[small_data['total_part_per_name'] > q]['asset_id'].tolist()
            small_data['event_date'] = pd.to_datetime(small_data['event_date'])
            small_data['original_install_date'] = pd.to_datetime(small_data['original_install_date'])
            small_data2 = small_data.groupby(['asset_id', 'investigation_id'], as_index=False).agg(
                {'part_cost': 'sum', 'labor_cost': 'max', 'event_date': 'max',
                 'original_install_date': 'max'}).sort_values(['asset_id', 'investigation_id', 'event_date'])
            # convert to datetime
            small_data2['diff_time'] = small_data2.groupby('asset_id')['event_date'].diff().dt.days
            first_visit = (small_data2['event_date'] - small_data2['original_install_date']).dt.days
            small_data2['diff_time'] = small_data2['diff_time'].fillna(first_visit)
            # cumulative diff time
            small_data2['cum_time'] = small_data2.groupby(['asset_id', 'investigation_id'])['diff_time'].sum() \
                .groupby(level=0).cumsum().reset_index()['diff_time']
            # total cost
            small_data2['total_cost'] = small_data2['part_cost'] + small_data2['labor_cost']
            # cumulative total cost
            small_data2['cum_cost'] = small_data2.groupby(['asset_id', 'investigation_id'])['total_cost'].sum() \
                .groupby(level=0).cumsum().reset_index()['total_cost']

            small_data2['time_class'] = np.where(small_data2['cum_time'] < (365 * 3), '3_year',
                                                 np.where(small_data2['cum_time'] < (365 * 6), '6_year',
                                                          np.where(small_data2['cum_time'] < (365 * 9), '9_year',
                                                                   'Empty')))
            small_data2['0.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 0.5), small_data2['cum_cost'], 0)
            small_data2['1_years_cost'] = np.where(small_data2['cum_time'] < (365 * 1.5), small_data2['cum_cost'], 0)
            small_data2['1.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 2), small_data2['cum_cost'], 0)
            small_data2['2_years_cost'] = np.where(small_data2['cum_time'] < (365 * 2.5), small_data2['cum_cost'], 0)
            small_data2['2.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 3), small_data2['cum_cost'], 0)
            small_data2['3_years_cost'] = np.where(small_data2['cum_time'] < (365 * 3.5), small_data2['cum_cost'], 0)
            small_data2['3.5-years_cost'] = np.where(small_data2['cum_time'] < (365 * 4), small_data2['cum_cost'], 0)
            small_data2['4_years_cost'] = np.where(small_data2['cum_time'] < (365 * 4.5), small_data2['cum_cost'], 0)
            small_data2['4.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 5), small_data2['cum_cost'], 0)
            small_data2['5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 5.5), small_data2['cum_cost'], 0)
            small_data2['5.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 6), small_data2['cum_cost'], 0)
            small_data2['6_years_cost'] = np.where(small_data2['cum_time'] < (365 * 6.5), small_data2['cum_cost'], 0)
            small_data2['6.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 7), small_data2['cum_cost'], 0)
            small_data2['7_years_cost'] = np.where(small_data2['cum_time'] < (365 * 7.5), small_data2['cum_cost'], 0)
            small_data2['7.5_years_cost'] = np.where(small_data2['cum_time'] < (365 * 8), small_data2['cum_cost'], 0)
            small_data2 = small_data2.drop('time_class', 1)
            # small_data2
            small_data2 = small_data2.groupby('asset_id', as_index=False).agg(
                {'original_install_date': 'max', '9_year_cost': 'sum', '6_year_cost': 'sum', '3_year_cost': 'sum'})
            # small_data2.filter(small_data2['asset_id'] != list_of_asset_out)
            # rows = small_data2.loc[small_data2.asset_id[list_of_asset_out]]
            small_data2 = small_data2[~small_data2['asset_id'].isin(list_of_asset_out)]
            #print the csv
            file_dir = os.path.dirname(os.path.abspath(__file__))
            small_data2.to_csv(os.path.join(file_dir, 'awesome.csv')
)
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