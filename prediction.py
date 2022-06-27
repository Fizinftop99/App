import numpy as np
import pandas as pd
from neo4j import work


class Work:
    def __init__(self, gesn: str):
        self.gesn = gesn
        a = gesn.split('-')
        self.chap, self.vol = [int(i) for i in a[0].split('.')]
        self.work_type = int(a[1])
        self.work = int(a[2])
        self.value = 1000 * self.chap + 100 * self.vol + 10 * self.work_type + self.work


def value(el: Work):
    return 1000 * el.chap + 100 * el.vol + 10 * el.work_type + el.work


def parsing(gesn: str):
    a = gesn.split('-')
    chap, vol = [int(i) for i in a[0].split('.')]  # two elements in a list
    work_type = int(a[1])
    work = int(a[2])


def main():
    df = pd.read_excel("data/2022-02-07 МОЭК_ЕКС график по смете.xlsx")
    gesns = df.loc[pd.notna(df['ADCM_шифрГЭСН']), 'ADCM_шифрГЭСН'].to_numpy()
    # print(gesns)
    # gesns = ['6.68-30-1',
    #          '6.68-30-2',
    #          '6.68-30-3',
    #          '6.68-30-5',
    #          '6.68-31-1',
    #          '6.68-31-2',
    #          '6.68-31-3',
    #          '6.68-31-5',
    #          '3.7-21-1',
    #          '6.68-13-1',
    #          '6.68-73-2'
    #          ]

    # works = {Work(i): value(Work(i)) for i in gesns}
    # print(min(works, key=works.get).gesn)

    chapters = dict()
    volumes = dict()
    work_types = dict()
    works = dict()

    for i in gesns[:3]:
        wrk = Work(i)
        if wrk.chap not in chapters:
            chapters[wrk.chap] = [wrk.vol]
    print(chapters)


if __name__ == '__main__':
    main()
