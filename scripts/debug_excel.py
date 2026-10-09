import pandas as pd

path = r'c:\ETL\snowglake to 4D\4D_to_Snowflake 1-50 tables mapping.xlsx'
print('Opening workbook...')
xl = pd.ExcelFile(path)
print('Sheets:', xl.sheet_names[:10])

sheet = 'Snowflake_table_names'
df = pd.read_excel(path, sheet_name=sheet)
print('\nColumns:', list(df.columns))
print('\nRows count:', len(df))
print(df.head(10).to_string(index=False))
