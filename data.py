from pathlib import Path
from tdc.multi_pred import DTI
from tdc.utils import retrieve_dataset_names


# folder
pth = Path('data/dataset')
pth.mkdir(exist_ok=True, parents=True)

# load datasets
dt_names = retrieve_dataset_names('DTI')
for dn in dt_names:
    print("====="*10)
    print(dn)
    data = DTI(name=dn)
    data.print_stats()
    data.convert_to_log(form='binding')
    for spt in ['random', 'cold_split']:
        if spt == 'random':
            split = data.get_split(method=spt, seed=42, frac=[0.8, 0.1, 0.1])
            for s in ['train', 'valid', 'test']:
                split[s].to_csv(pth / f'{dn}_{spt}_{s}.csv', index=False)
        else:
            for cold in ['Drug', 'Target', ['Drug', 'Target']]:
                split = data.get_split(method=spt, column_name=cold, seed=42, frac=[0.8, 0.1, 0.1])
                if isinstance(cold, list):
                    cold = '-'.join(cold)
                for s in ['train', 'valid', 'test']:
                    split[s].to_csv(pth / f'{dn}_{spt}_cold-{cold}_{s}.csv', index=False)
print("Done")