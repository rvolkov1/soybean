import pandas as pd


soybeans_df = pd.read_csv("soybeans.csv")

print(soybeans_df.shape)

soybeans_df = soybeans_df[['Year', 'State ANSI', 'County ANSI', 'Value']].dropna()
pivoted = soybeans_df.pivot(index=['State ANSI', 'County ANSI'], columns='Year', values='Value')


pivoted.to_csv('pivoted_soybeans')

