import pandas as pd
from pathlib import Path

tst_origin = pd.read_csv("../leash-BELKA/test_seq.csv")
# tst_origin = pd.read_csv("./leash-BELKA/test_seq.csv")
print(f'origin test len: {len(tst_origin)}')

total = []
for t in ['BRD4', 'HSA', 'sEH']:
    folder = f'./{t}/tst_result.csv'
    # folder = f'DrugBAN/{t}/tst_result.csv'
    df = pd.read_csv(folder)
    total.append(df)

total = pd.concat(total)
try:
    total = total.drop(columns=['Unnamed: 0', 'sigmoid'])
except:
    total = total.drop(columns=['sigmoid'])
total = total.sort_values(by=['id'], ascending=True)
total.to_csv('submission.csv', index=False)
print(f'generate submission: {len(total)}')
print('done')
