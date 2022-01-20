
from warehouse.inbound.core1 import *
from warehouse.inbound.input import *

data = pd.read_csv(wh_path)
data.drop(columns=['shopid', 'shop_name', 'main_category', 'vnw_cc_stk', 'vns_stk',
                   'pct_spx', 'pct_south', 'pct_hcm', 'stock_vol'], inplace=True)

# pre-process data
data['vnw_stk'].fillna(0, inplace=True)
data['color'].fillna('Yellow', inplace=True)
data['uid'] = [i for i in range(len(data))]
data['volume_unit'] = data['height'] * data['width'] * data['length'] / 1000000
data['distribution'] = data['color'].map(dict_color)

# check moving sku
group_sku = sum_stock(data, safety_stock)
data = data.merge(group_sku, on='sku_id', how='left')

# stats
data['volume_move'] = data['volume_unit'] * data['move']
data['volume_s1'] = data['volume_unit'] * data['vnw_stk']

volume_wh = 2200
total_volume_s1 = data['volume_s1'].sum()
move = -volume_wh + total_volume_s1
volume_move = data['volume_move'].sum()
print(f'total volume of warehouse: {int(total_volume_s1)} vs capacity : {volume_wh}')
print(f'total volume of sku can move: {int(volume_move)}, trucks: {int(volume_move / 16)}')
print(f'total volume of skus need to move: {int(move)}, trucks: {int(move / 16)}')
print()

raw = data.copy()

# ranking
data = data[data['move'].notnull()]
data.sort_values(by=['sku_id', 'exp_date', 'ib_date'], ascending=False, inplace=True)

data['prior_cover'] = data['cover_day'].apply(cover_day_cal)
data['prior_spx'] = data['spx_enabled'].map(dict_spx)
data['prior_color'] = data['color'].map(dict_color_priority)
data['prior_size'] = data['sku_size_type'].apply(lambda x: int(x[-1:]))
data['prior_ex_in_day'] = set_expiry_date(data)

data = choose_sku_take_out(data)
data = data[data['take_out'] != 0]
data = get_rank(data)
data.sort_values(by=['rank'], inplace=True)

# create batch to make sure enough sku on each rank
batch = create_batch(data)
optimize_df = {}
for ba in batch.keys():
    skus_batch = len(batch[ba])
    volume_batch = batch[ba]['volume_move'].sum()
    trucks = math.ceil(volume_batch / 16)
    print()
    print(f'batch {ba} has skus: {skus_batch}, volume: {volume_batch}, trucks: {trucks}')

    # run optimize
    test = batch[ba].copy()
    optimize_df[ba] = pd.DataFrame()
    for tr in range(1, trucks):
        final = optimize(test, tr, ba)
        optimize_df[ba] = optimize_df[ba].append(final)
        drop_sku = final['uid'].values.tolist()
        test.drop(drop_sku, inplace=True)

        left_sku = test.shape[0]
        left_sku_volume = test['volume_move'].sum()
        print(f'{left_sku} skus left, volume: {left_sku_volume}')
        if left_sku == 0:
            break

# result
final = pd.concat([optimize_df[f] for f in optimize_df.keys()])

result = raw.merge(final, how='left', on='uid')
result['error'] = result['vnw_stk'] - result['quantity']

result['volume_truck'] = result['quantity'] * result['volume_unit']
result.sort_values(by=['batch', 'truck'], inplace=True)
result.to_csv(f'result_current_{day}.csv', encoding='utf-8-sig', index=False)
