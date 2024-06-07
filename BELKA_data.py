import duckdb
import pandas as pd

train_path = 'leash-BELKA/train.parquet'
test_path = 'leash-BELKA/test.parquet'

# load train
con = duckdb.connect()

# trn = con.query(f"""(SELECT *
#                         FROM parquet_scan('{train_path}')
#                         WHERE binds = 0
#                         ORDER BY random()
#                         LIMIT 2000000)
#                         UNION ALL
#                         (SELECT *
#                         FROM parquet_scan('{train_path}')
#                         WHERE binds = 1
#                         ORDER BY random()
#                         LIMIT 2000000)""").df()

trn = con.query(f"""(SELECT *
                        FROM parquet_scan('{train_path}')
                        WHERE binds = 0
                        ORDER BY random()
                        LIMIT 30000)
                        UNION ALL
                        (SELECT *
                        FROM parquet_scan('{train_path}')
                        WHERE binds = 1
                        ORDER BY random()
                        LIMIT 30000)""").df()

con.close()

# load test
con2 = duckdb.connect()

tst = con2.query(f"""(SELECT *
                        FROM parquet_scan('{test_path}')
                        ORDER BY random()
                        LIMIT 2000000)
                        """).df()

con2.close()


# protein_seq
from Bio import SeqIO
seq = SeqIO.parse('fasta/protein.fasta', 'fasta')
prot_dict = {"sp|O60885|BRD4_HUMAN": 'BRD4', "sp|P02768|ALBU_HUMAN":"HSA", "sp|P34913|HYES_HUMAN": 'sEH'}
prot_seq = {}
for i in seq:
    id = prot_dict[i.id]
    prot_seq[id] = str(i.seq)


# filter
def filter_db(df, ps):
    result = df.copy()
    result.drop(columns=['buildingblock1_smiles', 'buildingblock2_smiles', 'buildingblock3_smiles'], inplace=True)
    result['Protein'] = result['protein_name'].map(ps)
    return result

ft_trn = filter_db(trn, prot_seq)
ft_tst = filter_db(tst, prot_seq)

import numpy as np
for protein_name in ft_trn['protein_name'].unique():
    s_trn = ft_trn[ft_trn['protein_name'] == protein_name]
    s_tst = ft_tst[ft_tst['protein_name'] == protein_name]
    s_tst['Y'] = np.random.randint(0, 2, size=len(s_tst))
    s_trn.to_csv(f'leash-BELKA/train_{protein_name}.csv', index=False)
    s_tst.to_csv(f'leash-BELKA/test_{protein_name}.csv', index=False)
    print(protein_name, 'done')

# save
ft_trn.to_csv('leash-BELKA/train_seq.csv', index=False)
ft_tst.to_csv('leash-BELKA/test_seq.csv', index=False)
print('done')
