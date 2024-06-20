import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')


train_path = 'leash-BELKA/train.parquet'
test_path = 'leash-BELKA/test.parquet'

# load train
con = duckdb.connect()

trn = con.query(f"""(SELECT *
                        FROM parquet_scan('{train_path}')
                        WHERE binds = 0
                        ORDER BY random()
                        LIMIT 2000000)
                        UNION ALL
                        (SELECT *
                        FROM parquet_scan('{train_path}')
                        WHERE binds = 1
                        ORDER BY random()
                        LIMIT 2000000)""").df()

# trn = con.query(f"""(SELECT *
#                         FROM parquet_scan('{train_path}')
#                         WHERE binds = 0
#                         ORDER BY random()
#                         LIMIT 30000)
#                         UNION ALL
#                         (SELECT *
#                         FROM parquet_scan('{train_path}')
#                         WHERE binds = 1
#                         ORDER BY random()
#                         LIMIT 30000)""").df()

con.close()

# load test
con2 = duckdb.connect()

tst = con2.query(f"""(SELECT *
                        FROM parquet_scan('{test_path}')
                        ORDER BY random()
                        LIMIT 2000000)
                        """).df()

con2.close()

# filter
def filter_db(df):
    result = df.copy()
    result.drop(columns=['buildingblock1_smiles', 'buildingblock2_smiles', 'buildingblock3_smiles'], inplace=True)
    return result

# def multi_label(df, tst=False):
#     result = []
#     uq = df['molecule_smiles'].unique()
#     for u in tqdm(uq):
#         tbinds = df[df['molecule_smiles'] == u]
#         new_row = {}
#         new_row['molecule_smiles'] = u
#         for t in ['BRD4', 'HSA', 'sEH']:
#             pt = tbinds[tbinds['protein_name'] == t]
#             if not tst:
#                 if not pt.empty:
#                     if len(pt) >= 2:
#                         print(f'{u} is duplicated!!!')
#                         print(pt)
#                     new_row[f'{t}_id'] = pt['id'].values[0]
#                     new_row[f"{t}_binds"] = pt['binds'].values[0]
#                 else:
#                     print(f'{u} - {t} is empty!!!')
#                     new_row[f'{t}_id'] = None
#                     new_row[f"{t}_binds"] = None
#             else:
#                 new_row[f"{t}_binds"] = np.random.randint(0, 2, size=1)
#         result.append(new_row)
#     result = pd.DataFrame(result).reset_index(drop=True)
#     return result

def multi_label(df, tst=False):
    result = []
    for t in ['BRD4', 'HSA', 'sEH']:
        tbinds = df[df['protein_name'] == t]
        tbinds[f"{t}_id"] = tbinds['id']
        if tst:
            tbinds[f"{t}_binds"] = np.random.randint(0, 2, size=len(tbinds))
            tbinds.drop(columns=['protein_name', 'id'], inplace=True)
        else:
            tbinds[f"{t}_binds"] = tbinds['binds']
            tbinds.drop(columns=['protein_name', 'id', 'binds'], inplace=True)
        print(tbinds.columns)
        tbinds = tbinds[['molecule_smiles', f'{t}_id', f'{t}_binds']]
        result.append(tbinds)
    merged_df = result[0].merge(result[1], on='molecule_smiles', how='outer').merge(result[2], on='molecule_smiles', how='outer')
    merged_df.reset_index(drop=True, inplace=True)
    return merged_df

ft_trn = multi_label(filter_db(trn))
ft_tst = multi_label(filter_db(tst), tst=True)

# save
ft_trn.to_csv('leash-BELKA/multi/train_multi.csv', index=False)
ft_tst.to_csv('leash-BELKA/multi/test_multi.csv', index=False)
print('done')
