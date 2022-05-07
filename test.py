import pandas as pd
import numpy as np


def read_graph_data(file_name: str) -> pd.DataFrame:
    target_cols = ['Идентификатор операции', 'Артикул', 'Название операции', 'Последователи']
    df = pd.read_excel(file_name, dtype=str)
    exist_cols = df.columns.values.tolist()
    if 'ADCM_П/п' in exist_cols:
        df.rename(columns={'ADCM_П/п': 'Артикул'}, inplace=True)

    df = df[target_cols]
    df = df.loc[df['Артикул'].notna()]
    df.loc[:, 'Идентификатор операции'] = df['Идентификатор операции'].apply(str.strip)
    df = df[df['Идентификатор операции'].str.startswith('R')]
    df.set_index('Идентификатор операции', inplace=True)  # Update indeces

    return df


def main():
    file_name = 'data/solution_schedule.xlsx'
    # df = pd.read_excel(file_name, sheet_name="Состав работ", dtype=str, index_col=0)
    df = pd.read_excel(file_name, sheet_name="Состав работ")
    df = df.drop(['Unnamed: 0'], axis=1, errors='ignore')
    print(df.dtypes)
    df = df.astype('str')
    print(df.dtypes)
    vendors = df['vendor_code'].to_numpy()
    # print(vendors)
    # print(df.columns)


if __name__ == '__main__':
    main()
