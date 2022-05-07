import pandas as pd
from neo4j import GraphDatabase, Transaction
import numpy as np
import datetime
from yml import get_cfg

src_uri = "neo4j+s://174cd36c.databases.neo4j.io:7687"
src_user = "neo4j"
src_password = "w21V4bw-6kTp9hceHMbnlt5L9X1M4upuuq2nD7tD_xU"
uri = "bolt://localhost:7687"
user = "neo4j"
password = "2310"


# src_uri = "neo4j://20.107.79.39:7687"
# src_user = "neo4j"
# src_password = "Accelerati0n"


class Neo4jExplorer:
    def __init__(self):
        # read settings from config
        self.cfg: dict = get_cfg("neo4j")
        _uri = self.cfg.get('uri')
        _user = self.cfg.get('user')
        _pass = self.cfg.get('password')
        # print(_uri, _user, _pass, sep='\n')

        self.driver = GraphDatabase.driver(_uri, auth=(_user, _pass))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()
        # print('closed')

    def load_data(self):
        # driver to historical database
        _hist_uri = self.cfg.get('src_uri')
        _hist_user = self.cfg.get('src_user')
        _hist_pass = self.cfg.get('src_password')
        _hist_driver = GraphDatabase.driver(_hist_uri, auth=(_hist_user, _hist_pass))

        q_data_obtain = '''
            MATCH (n)-[r]->(m)
            RETURN n.name AS n_name, n.id AS n_id, properties(r).weight AS weight, m.name AS m_name, m.id AS m_id
            '''
        q_create = '''
            LOAD CSV WITH HEADERS FROM 'file:///2.csv' AS row
            MERGE (n:Work {id: row.n_id, name: row.n_name})
            MERGE (m:Work {id: row.m_id, name: row.m_name})
            CREATE (n)-[r:FOLLOWS {weight: row.weight}]->(m);
            '''

        # obtaining data
        result = _hist_driver.session().run(q_data_obtain).data()
        _hist_driver.close()

        df = pd.DataFrame(result)
        save_path = 'C:\\Users\\Nikita\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-44d5c24b-bb41-4e4b-bbeb-f18bca851f09' \
                    '\\import\\'
        df.to_csv(save_path + '2.csv', index=False)
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")  # Очистка
            session.run(q_create)

    def removing_node(self, id: str):
        income_data_obtain = '''
            MATCH (n)-[]->(m)
            WHERE m.id = $id
            RETURN n
            '''
        outcome_data_obtain = '''
            MATCH (n)-[]->(m)
            WHERE n.id = $id
            RETURN m
            '''
        with self.driver.session() as session:
            incoming = session.run(income_data_obtain, id=id).data()
            outcoming = session.run(outcome_data_obtain, id=id).data()
            # преобразование результатов запроса в numpy.array
            incoming = np.array([row['n']['id'] for row in incoming])
            outcoming = np.array([row['m']['id'] for row in outcoming])

            for element in incoming:
                for subelement in outcoming:
                    session.run('''
                                        MERGE (n:Work {id: $id1})
                                        MERGE (m:Work {id: $id2})
                                        MERGE (n)-[r:FOLLOWS]->(m)
                                        ''',
                                id1=element,
                                id2=subelement
                                )
            session.run("MATCH (n) WHERE n.id = $id DETACH DELETE n", id=id)

    def get_all_id(self):
        q_data_obtain = '''
            MATCH (n)
            RETURN n
            '''
        result = self.driver.session().run(q_data_obtain).data()
        id_lst = []
        for i in result:
            id_lst.append((i['n']['id']))
        return list(set(id_lst))

    def new_graph(self, target_ids: list):
        for element in self.get_all_id():
            if element not in target_ids:
                self.removing_node(element)

    def create_link(self, tx: Transaction, parent_id, child_id):
        tx.run("MATCH (parent) WHERE parent.id = $parent_id "
               "MATCH (child) WHERE child.id = $child_id "
               "MERGE (parent)-[r]->(child)",
               parent_id=parent_id,
               child_id=child_id)

    def remove_link(self, tx: Transaction, parent_id, child_id):
        tx.run("MATCH (parent) WHERE parent.id = $parent_id "
               "MATCH (child) WHERE child.id = $child_id "
               "DELETE (parent)-[r]->(child)",
               parent_id=parent_id,
               child_id=child_id)

    def way_exist(self, parent_id, child_id):
        query = "CALL gds.graph.project('myGraph', 'Node', 'REL')"
        query = '''MATCH (a:Node{name:'A'}), (b:Node{name:'B'})
                   WHERE a.id = $parent_id AND b.id = $child_id
                   WITH id(a) AS source, [id(b)] AS targetNodes
                   CALL gds.dfs.stream('myGraph', {
                   sourceNode: source,
                   targetNodes: targetNodes
                   })
                   YIELD path
                   RETURN path'''

    def triangle_destroyer(self, parent_id, child_id):
        outcome_data_obtain = f'''
                            MATCH (n)-[]->(m)
                            WHERE n.id = $id
                            RETURN m
                            '''

        with self.driver.session() as session:

            outcoming = session.run(outcome_data_obtain, id=id).data()
            outcoming = np.array([row['m']['id'] for row in outcoming])

            for child in outcoming:
                self.driver.session().write_transaction(self.remove_link, parent_id, child_id)
                if not self.way_exist(parent_id, child_id):
                    self.driver.session().write_transaction(self.create_link, parent_id, child_id)

    def del_extra_rel(self):
        q_delete = '''
            match (b)<-[r]-(a)-->(c)-->(b)
            delete r
            '''
        self.driver.session().run(q_delete)

    def ordering(self, schedDF: pd.DataFrame):
        q_data_obtain = '''
        MATCH (n)-[]->(m)
        RETURN n.id AS n_id, m.id AS m_id
        '''
        schedDF.drop(['Unnamed: 0'], axis=1, errors='ignore', inplace=True)
        schedDF = schedDF.astype('str')
        schedDF['precursors'] = ''
        schedDF['followers'] = ''
        wbs2_arr = schedDF.wbs2.unique()

        for i_wbs2 in wbs2_arr:
            wbs_df = schedDF[schedDF.wbs2 == i_wbs2]
            vendors = wbs_df.vendor_code.to_numpy()
            self.load_data()
            self.new_graph(vendors)
            self.del_extra_rel()

            result = self.driver.session().run(q_data_obtain).data()
            df = pd.DataFrame(result)
            for vend in vendors:
                ind2 = wbs_df.index[wbs_df.vendor_code == vend].tolist()[0]
                flwDF = df.loc[df.n_id == vend]
                if not flwDF.empty:
                    flws = flwDF.m_id.to_numpy()
                    schedDF.at[ind2, 'followers'] = ', '.join(flws)

                predDF = df.loc[df.m_id == vend]
                if not predDF.empty:
                    preds = predDF.n_id.to_numpy()
                    schedDF.at[ind2, 'precursors'] = ', '.join(preds)

        return schedDF


def main():
    # starttime = datetime.datetime.now()
    #
    # file_path = 'data/solution_schedule.xlsx'
    # schedDF = pd.read_excel(file_path, sheet_name="Состав работ", dtype=str, index_col=0)

    app = Neo4jExplorer()
    # resultDF = app.ordering(schedDF)
    app.close()
    #
    # with pd.ExcelWriter('data/result.xlsx', engine='openpyxl') as writer:
    #     resultDF.to_excel(writer, sheet_name="Упорядоченно")
    # print('data ordered', datetime.datetime.now() - starttime)


if __name__ == "__main__":
    main()
