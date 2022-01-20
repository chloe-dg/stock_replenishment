import pandas as pd
import math
import numpy as np
import pulp
from collections import defaultdict
from datetime import date

day = date.today().isoformat()

def optimize(df, num_truck, num_batch):
    lst = df.index.tolist()
    move_to = df['take_out'].to_dict()
    volume = df['volume_unit'].to_dict()

    """ VARIABLES & MODEL """
    x = pulp.LpVariable.dicts("x", lst, cat='Integer', lowBound=0)
    model = pulp.LpProblem("minimize", pulp.LpMinimize)
    model += sum([x[i] for i in x])

    """ CONSTRAINTS """
    # skus can move
    for idx in x:
        model += x[idx] <= move_to[idx]

    # volume on trucks
    model += sum(x[idx] * volume[idx] for idx in x) <= 16
    model += sum(x[idx] * volume[idx] for idx in x) >= 15
    """ SOLVER & OUTPUT"""
    model.solve()
    print(f'Status on truck {num_truck}: {pulp.LpStatus[model.status]}')

    output = []
    for i in x:
        var_output = {
            '#': i,
            'stock': x[i].varValue
        }
        output.append(var_output)

    output_df = defaultdict(list)
    for i in output:
        output_df[i['#']].append(i['stock'])
    final = pd.DataFrame(output_df).T.reset_index()
    final = final[final[0] > 0]
    final.columns = ['uid', 'quantity']
    final['truck'] = num_truck
    final['batch'] = num_batch

    if pulp.LpStatus[model.status] == 'Infeasible':
        final = final.iloc[0:0]

    return final


def set_expiry_date(df):
    lst_length = []
    for s in df['sku_id'].unique():
        length = df[df['sku_id'] == s].shape[0]
        lst_length.append([i for i in range(0, length)])
    return sum(lst_length, [])


def cover_day_cal(num):
    if num > 14:
        return 2
    elif 7 < num <= 14:
        return 1
    else:
        return 0


def create_batch(df):
    batch = {}
    consolidate = {}
    for r in df['rank'].unique():
        batch[r] = df[df['rank'] == r]
        batch[r].set_index('uid', inplace=True)

    count = 0
    consolidate[count] = pd.DataFrame()
    for i in batch.keys():
        vol = batch[i]['volume_move'].sum()
        if vol > 16:
            consolidate[count] = consolidate[count].append(batch[i])
            count += 1
            consolidate[count] = pd.DataFrame()
        else:
            consolidate[count] = consolidate[count].append(batch[i])
            vol_conso = consolidate[count]['volume_move'].sum()
            if vol_conso > 16:
                count += 1
                consolidate[count] = pd.DataFrame()

    print(f'total batch: {len(consolidate.keys())}')
    return consolidate


def sum_stock(df, safety_stock):
    agg_df = df.groupby(['sku_id', 'color', 'distribution']).sum()[['vnw_stk']].reset_index()

    agg_df['M_safety_stock'] = safety_stock
    agg_df['move'] = agg_df['vnw_stk'] - agg_df['M_safety_stock']
    agg_df['move'] *= agg_df['distribution']
    agg_df['move'] = agg_df['move'].apply(lambda x: max(int(x), 0))

    agg_df['cover_day'] = agg_df['vnw_stk'] / agg_df['M_safety_stock']
    agg_df = agg_df[agg_df['move'] > 0][['sku_id', 'move', 'cover_day']]

    return agg_df


def choose_sku_take_out(df):
    take_out = []
    for sku in df['sku_id'].unique():
        tmp_sku = df[df['sku_id'] == sku][['vnw_stk', 'uid']].values
        stock = df[df['sku_id'] == sku]['move'].values[0]
        lst = []
        for val in tmp_sku:
            s1 = val[0]
            uid = val[1]
            if stock > 0:
                if stock - s1 > 0:
                    lst.append((s1, uid))
                    stock -= s1
                else:
                    lst.append((stock, uid))
                    stock -= stock
            else:
                lst.append((0, uid))
        take_out.append(lst)

    take_df = pd.DataFrame(sum(take_out, []), columns=['take_out', 'uid'])
    df = df.merge(take_df, how='left', on='uid')

    return df


def get_rank(df):
    count = 0
    for cover in sorted(list(df['prior_cover'].unique())):
        for spx in sorted(list(df['prior_spx'].unique())):
            for color in sorted(list(df['prior_color'].unique())):
                for size in sorted(list(df['prior_size'].unique()), reverse=True):
                    for day in sorted(list(df['prior_ex_in_day'].unique())):
                        mask = (df['prior_cover'] == cover) & (df['prior_spx'] == spx) & (df['prior_color'] == color) & (df['prior_ex_in_day'] == day) & (df['prior_size'] == size)
                        df.loc[mask, 'rank'] = count
                        count += 1
    return df
