from neo4j import GraphDatabase, Transaction
import pandas as pd


URI = "neo4j+s://174cd36c.databases.neo4j.io"
USER_NAME = "neo4j"
PASSWD = "w21V4bw-6kTp9hceHMbnlt5L9X1M4upuuq2nD7tD_xU"


def read_graph_data(file_name: str) -> pd.DataFrame:
    target_cols = ['Идентификатор операции', 'Артикул', 'Название операции', 'Последователи']
    df = pd.read_excel(file_name, dtype=str)
    df = df[target_cols]
    df = df.loc[df['Артикул'].notna()]
    df.loc[:, 'Идентификатор операции'] = df['Идентификатор операции'].apply(str.strip)
    df = df[df['Идентификатор операции'].str.startswith('R')]
    df.set_index('Идентификатор операции', inplace=True)  # Update indeces

    return df


def make_graph(tx: Transaction, data: pd.DataFrame):
    id_lst = data.index.tolist()
    for wrk_id in id_lst:
        tx.run("MERGE (a:Work {id: $id}) "
               "SET a.name = $name",
               id=data.loc[wrk_id, 'Артикул'],
               name=data.loc[wrk_id, 'Название операции'])

        s = data.loc[wrk_id, 'Последователи']
        if s == s:  # Проверка на то, что есть Последователи (s != NaN)
            followers = s.split(', ')
            for flw_id in followers:
                if flw_id in id_lst:  # Создаем ребра только к вершинам, описанным в таблице отдельной строкой
                    tx.run("MATCH (a:Work) WHERE a.id = $wrk_id "
                           "MERGE (flw:Work {id: $flw_id}) "
                           "SET flw.name = $flw_name "
                           "MERGE (a)-[r:FOLLOWS]->(flw) "
                           "SET r.weight = coalesce(r.weight, 0) + 1",
                           wrk_id=data.loc[wrk_id, 'Артикул'],
                           flw_id=data.loc[flw_id, 'Артикул'],
                           flw_name=data.loc[flw_id, 'Название операции'])


def clear_database(tx: Transaction):
    tx.run("MATCH (n) "
           "DETACH DELETE n")


def main():
    # data_file_paths = ["data/2021-11-19 Roder связи.xlsx", "data/Родер - КТК.xlsx"]
    data_file_paths = ["data/Родер - КТК.xlsx"]
    # data_file_paths = ["data/2021-11-19 Roder связи.xlsx"]
    # driver = GraphDatabase.driver(URI, auth=(USER_NAME, PASSWD))
    driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))

    with driver.session() as session:
        session.write_transaction(clear_database)
        for i in data_file_paths:
            data = read_graph_data(i)
            session.write_transaction(make_graph, data)
    driver.close()


if __name__ == "__main__":
    main()


# driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
# driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "2310"))

# exist_cols = df.columns.values.tolist()
# if 'ADCM_П/п' in exist_cols:
#     df.rename(columns={'ADCM_П/п': 'Артикул'}, inplace=True)

