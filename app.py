import pandas as pd
from neo4j import GraphDatabase
import numpy as np
import datetime
import xlsxwriter


class App:
    def __init__(self, uri_, user_, password_):
        self.driver = GraphDatabase.driver(uri_, auth=(user_, password_))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def load_data(self, remote_uri, remote_user, remote_pswd):
        q_data_obtain = '''
                        MATCH (n)-[]->(m)
                        RETURN n, m
                        '''
        q_create = '''
                MERGE (n:Work {id: $n_id, name: $n_name}) 
                MERGE (m:Work {id: $m_id, name: $m_name})
                MERGE (n)-[r:FOLLOWS]->(m)
                '''
        # obtaining data
        src_driver = GraphDatabase.driver(remote_uri, auth=(remote_user, remote_pswd))
        result = src_driver.session().run(q_data_obtain).data()
        src_driver.close()

        n_id_arr = np.array([r['n']['id'] for r in result])
        m_id_arr = np.array([r['m']['id'] for r in result])
        n_name_arr = np.array([r['n']['name'] for r in result])
        m_name_arr = np.array([r['m']['name'] for r in result])
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")  # Очистка
            for n_id, n_name, m_id, m_name in zip(n_id_arr, n_name_arr, m_id_arr, m_name_arr):
                session.run(q_create,
                            n_id=n_id,
                            n_name=n_name,
                            m_id=m_id,
                            m_name=m_name
                            )

    def removing_node(self, id: str):
        income_data_obtain = f'''
                MATCH (n)-[]->(m)
                WHERE m.id = $id
                RETURN n
                '''
        outcome_data_obtain = f'''
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

    def result_to_excel(self):
        q_data_obtain = '''
                        MATCH (n)-[]->(m)
                        RETURN n, m
                        '''
        result = self.driver.session().run(q_data_obtain).data()
        n_id_arr = np.array([r['n']['id'] for r in result])
        m_id_arr = np.array([r['m']['id'] for r in result])
        n_name_arr = np.array([r['n']['name'] for r in result])
        m_name_arr = np.array([r['m']['name'] for r in result])
        preds = dict()
        flws = dict()
        names = dict()
        for n_id, n_name, m_id, m_name in zip(n_id_arr, n_name_arr, m_id_arr, m_name_arr):
            names[n_id] = n_name
            names[m_id] = m_name
            try:
                flws[n_id].add(m_id)
            except KeyError:
                flws[n_id] = {m_id}
            try:
                preds[m_id].add(n_id)
            except KeyError:
                preds[m_id] = {n_id}
        workbook = xlsxwriter.Workbook('result.xlsx')
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, 'Идентификатор операции')
        worksheet.set_column(0, 0, 25)
        worksheet.write(0, 1, 'Название операции')
        worksheet.set_column(1, 1, 75)
        worksheet.write(0, 2, 'Предшественники')
        worksheet.write(0, 3, 'Последователи')
        worksheet.set_column(2, 3, 30)
        row = 1
        for i in self.get_all_id():
            worksheet.write(row, 0, i)
            worksheet.write(row, 1, names[i])
            try:
                worksheet.write(row, 2, ', '.join(preds[i]))
            except KeyError:
                worksheet.write(row, 2, '')
            try:
                worksheet.write(row, 3, ', '.join(flws[i]))
            except KeyError:
                worksheet.write(row, 3, '')
            row += 1
        workbook.close()


def main():
    starttime = datetime.datetime.now()
    src_uri = "neo4j+s://174cd36c.databases.neo4j.io"
    src_user = "neo4j"
    src_password = "w21V4bw-6kTp9hceHMbnlt5L9X1M4upuuq2nD7tD_xU"
    # uri = "bolt://localhost:7687"
    # user = "neo4j"
    # password = "2310"
    uri = "neo4j://20.107.79.39:7687"
    user = "neo4j"
    password = "Accelerati0n"

    app = App(uri, user, password)
    app.load_data(src_uri, src_user, src_password)
    print(datetime.datetime.now() - starttime)
    app.new_graph(['100203',  # Монтаж планка натягивающая
                   '171670',  # Монтаж трос стен вi-l дл.5986 серьга вверху
                   '192057',  # Монтаж алюминиевая направляющая для пола bi-level ii-уровень
                   '111281',  # Монтаж балка несущая вi-l5м дл.
                   '161564'  # Монтаж панель стеклянная
                   ])
    print(datetime.datetime.now() - starttime)
    app.result_to_excel()
    app.close()
    print(datetime.datetime.now() - starttime)


if __name__ == "__main__":
    main()
