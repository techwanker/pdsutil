from pdsutil.DbUtil import SqlStatements
import argparse

class InteractiveSql:

    def __init__(self, yaml_file_name:str):
        if yaml_file_name is not None:
            self.statements = SqlStatements.from_yaml(yaml_file_name)
        else:
            self.statements = SqlStatements.from_statement_list([])

    def interactive(self):
        statements_by_index = {}
        index = 1
        for k in self.statements:
            statements_by_index[index] = k
        print(self.enumerate_statements())
        s = input("choose a statement")
        print(statements_by_index[int(s)])

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("--connection_name", required=False, default="test",
                        help="name of database connection")
