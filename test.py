import csv

import pandas as pd
import numpy as np
from neo4j import GraphDatabase, Transaction


def main():
    q_data_obtain = '''
                    MATCH (n)-[r]->(m)
                    RETURN n.name AS name1, n.id AS id1, properties(r).weight AS weight, m.name AS name2, m.id AS id2
                    '''
    driver = GraphDatabase.driver("neo4j://20.107.79.39:7687", auth=("neo4j", "Accelerati0n"))
    with driver.session() as session:
        result = session.run(q_data_obtain).data()
        df = pd.DataFrame(result)
        save_path = 'C:\\Users\\Nikita\\.Neo4jDesktop\\relate-data\\dbmss\\dbms-44d5c24b-bb41-4e4b-bbeb-f18bca851f09\\import\\'
        df.to_csv(save_path + '2.csv', index=False)
    driver.close()

    # with open(, 'w') as f:
    #     writer = csv.writer(f)
    #     header = ['n_id', 'n_name', 'r_weight', 'm_id', 'm_name']
    #     for i in result:
    #         row = [i[]]
    #         writer.writerow(row)


if __name__ == '__main__':
    main()
