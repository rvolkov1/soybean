import pandas as pd

county_fips_df = pd.read_csv('county_fips.csv')

county_fips_df[['GeoFIPS', 'Region']] = county_fips_df['GeoFIPS      Region'].str.split(' ', n=1, expand=True)

county_fips_df = county_fips_df[['GeoFIPS', 'Region']]
county_fips_df['Region'] = county_fips_df['Region'].str.strip()
county_fips_df['GeoFIPS'] = pd.to_numeric(county_fips_df['GeoFIPS'])



soybeans_df = pd.read_csv("soybeans.csv")

soybeans_df = soybeans_df[['Year', 'State ANSI', 'County ANSI', 'Value']].dropna()
soybeans_df['GeoFIPS'] = (soybeans_df['State ANSI'] * 1000 + soybeans_df['County ANSI']).astype(int)
#pivoted_soybeans_df = soybeans_df.pivot(index=['State ANSI', 'County ANSI'], columns='Year', values='Value')
pivoted_soybeans_df = soybeans_df.pivot(index='GeoFIPS', columns='Year', values='Value')


print(pivoted_soybeans_df.columns)
#pivoted_soybeans_df['GeoFIPS'] = (pivoted_soybeans_df['State ANSI'] * 1000 + pivoted_soybeans_df['County ANSI']).astype(int)

pivoted_soybeans_df = pd.merge(county_fips_df, pivoted_soybeans_df, on = 'GeoFIPS', how='right')
pivoted_soybeans_df.to_csv('pivoted_soybeans.csv', index=False)




gdp_df = pd.read_csv("gdp.csv")

gdp_df = gdp_df.drop(gdp_df.columns[[1, 2, 3, 4, 5, 7]], axis=1)

gdp_df['GeoFIPS'] = pd.to_numeric(gdp_df['GeoFIPS'].str[2:7])


gdp_df = pd.merge(county_fips_df, gdp_df, on = 'GeoFIPS', how='right')

total_gdp_df = gdp_df[gdp_df['Description'].astype(str).str.strip().eq("All industry total")]
ag_gdp_df = gdp_df[gdp_df['Description'].astype(str).str.strip().eq("Agriculture, forestry, fishing and hunting")]

total_gdp_df = total_gdp_df.drop('Description', axis=1)
ag_gdp_df = ag_gdp_df.drop('Description', axis=1)


ag_gdp_df.to_csv('ag_gdp.csv', index=False)
total_gdp_df.to_csv('total_gdp.csv', index=False)











