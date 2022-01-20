import pathlib

dict_color = {
    'Black': 0.8,
    'Grey': 0.6,
    'Red': 0.5,
    'Yellow': 0.3,
    'Green': 0.2
}

dict_spx = {
    'enabled': 1,
    'disabled': 0
}

dict_color_priority = {
    'Black': 0,
    'Grey': 1,
    'Red': 2,
    'Yellow': 3,
    'Green': 4
}

safety_stock = 50

wh_path = list(pathlib.Path('C:/Users/shopeevn/PycharmProjects/modeling/warehouse/inbound/data').glob('inventories*.csv'))[0]
inbound_path = 'data/po_ib.csv'
