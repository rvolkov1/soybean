import pandas as pd

soybeans_df = pd.read_csv("soybeans.csv")

soybeans_df = soybeans_df[['Year', 'State ANSI', 'County ANSI', 'Value']].dropna()
pivoted_soybeans_df = soybeans_df.pivot(index=['State ANSI', 'County ANSI'], columns='Year', values='Value')

pivoted_soybeans_df.to_csv('pivoted_soybeans.csv')


gdp_df = pd.read_csv("gdp.csv")

gdp_df = gdp_df.drop(gdp_df.columns[[1, 2, 3, 4, 5, 7]], axis=1)


total_gdp_df = gdp_df[gdp_df['Description'].astype(str).str.strip().eq("All industry total")]
ag_gdp_df = gdp_df[gdp_df['Description'].astype(str).str.strip().eq("Agriculture, forestry, fishing and hunting")]

total_gdp_df = total_gdp_df.drop('Description', axis=1)
ag_gdp_df = ag_gdp_df.drop('Description', axis=1)

ag_gdp_df.to_csv('ag_gdp.csv', index=False)
total_gdp_df.to_csv('total_gdp.csv', index=False)
