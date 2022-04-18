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
    df = read_graph_data("data/2021-11-19 Roder связи.xlsx")
    df2 = read_graph_data("data/Родер - КТК.xlsx")
    with pd.ExcelWriter('check.xlsx', mode='w') as writer:
        df.to_excel(writer, sheet_name='1')
        df2.to_excel(writer, sheet_name='2')

        writer.sheets['1'].set_column(0, 0, 25)
        writer.sheets['1'].set_column(2, 2, 75)
        writer.sheets['1'].set_column(3, 3, 30)

        writer.sheets['2'].set_column(1, 1, 30)
        writer.sheets['2'].set_column(2, 2, 75)
        writer.sheets['2'].set_column(3, 3, 30)

    vend1 = df['Артикул'].to_numpy()
    vend2 = df2['Артикул'].to_numpy()
    common = np.intersect1d(vend1, vend2)
    print(common)


if __name__ == '__main__':
    main()
