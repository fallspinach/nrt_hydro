import pandas as pd
import sys

f = sys.argv[1]

df = pd.read_csv(f, compression='gzip')
df.rename(columns={'time': 'Date'}, inplace=True)
df.to_csv(f, compression='gzip', index=False, float_format='%.4f')

