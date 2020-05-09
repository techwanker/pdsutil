#!/usr/bin/python
from typing import Dict,List # Tuple
import logging
import datetime



from pdsutil.UtConditionPersistence import UtConditionRunPersistence, UtConditionRowMsgPersistence, \
    UtConditionRunStepPersistence
from pdsutil.DbUtil import Cursors

logger = logging.getLogger(__name__)


class ConditionIdentification:
    """
    Runs a series of sql statements that return as the first column, the
    primary key of a table being examined an 0 or more fields used in formatting
    a message.
    """



    insert_cursor = None

    rules = {}

    messages = []

    def __init__(self, conn, rules:List[Dict[str,str]], verbose=False):
        """
        :param conn: A database connection
        :param rules: A list of ConditionIdentification
        :param verbose:
        """
        self.connection = conn
        self.cursors = Cursors(self.connection)
        self.rules = rules
        self.verbose = verbose
        logger.debug("instantiated")

    def add_rule(self, rule:Dict[str,str]):
        self.rules[rule.rule_name] = rule

    def print_messages(self):
        for message in self.messages:
            print(message)



    def process(self, binds:Dict[str,object], verbosity=0):
        """
        :param self:
        :param binds: dictionary of bind names and variables
        :param verbosity: int logging level

        :return:  List of str - generated messages

        Each invocation starts a new run.



        """
        # *   A row is inserted into ut_condition_run  TODO get to work in sphinx
        #     *   The bind variables are inserted into ut_condition_run_parm
        #         * for each rule
        #             * The rule is retrieved from ut_condition and created if necessary.
        #             * For each row returned
        #             * a row is inserted into ut_condition_row_msg
        #             * A row is inserted into ut_condition_run_step
        self.messages = []
        ut_condition_run_id = UtConditionRunPersistence.start_run(self.cursors, binds)  # Record the start of the run
        rule_cursor = self.cursors.get_cursor("rule_sql")
        for rule in self.rules:
            rule_name = rule["rule_name"]
            start_ts = datetime.datetime.now()
            condition_sql = rule["sql_text"]
            rows = rule_cursor.execute(condition_sql, binds)
            run_step_id = UtConditionRunStepPersistence.insert(
                self.cursors,ut_condition_run_id,start_ts,rule)

            row_count = 0
            for row in rows:
                row_count += 1
                msg = rule["format_str"] % row  # TODO LITERAL
                try:
                    #def insert(cursors: Cursors, ut_condition_run_step_id: int, primary_key: int, message: str) -> None:
                    UtConditionRowMsgPersistence.insert(self.cursors,run_step_id, row[0], msg)
                except Exception as e:
                    raise Exception("While processing\n%s\n%s" % (condition_sql, str(e)))

                self.messages.append(msg)
                # if verbosity > 0:
                #     logger.info(msg)
            end_ts = datetime.datetime.now()
            if verbosity > 0:
               #{: < 7}{: < 51}{: < 25}\n
                logger.info("Rule " + "{:<30}".format(rule_name) + " run_step: " + str(run_step_id).rjust(6) +
                            " Number of exceptions: " + "{:>6}".format(row_count))
                    #    (rule_name, run_step_id, row_count)))
               # logger.info("Rule %s run_step: %s Number of exceptions: %s" %
        self.connection.commit()
        self.cursors.close()
        #self.connection.close()
        # if verbosity > 2:
        #     self.print_messages()
        return self.messages
