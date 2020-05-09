from typing import Dict, Tuple, List
class ConditionIdentificationRule:
    RULE_NAME = "rule_name"
    TABLE_NAME = "table_name"
    MSG = "msg"
    SQL= "sql"
    FORMAT = "format"

    def __init__(self, rule_name:str, table_name:str, msg:str, sql_text:str,
                 format_str:str=None, description:str=None, corrective_action:str=None, severity:int=None) -> None:
        """

        :param rule_name: str - Textual identifier of the rule
        :param table_name: str - The name of the table from which the primary key
               is being returned
        :param msg:
        :param sql_text: The first column should be a primary key for the table_name
        :param format_str: a python format string to format a message based on columns
               returned in the select
        :param description: documentation of condition being identified
        :param corrective_action: what should be done to correct this action
        :param severity: 0 - 9 used by utilities reading
        """

        self.rule_name = rule_name
        self.sql_text = sql_text
        self.msg = msg
        self.table_name = table_name
        self.format_str = format_str
        self.description = description
        self.corrective_action = corrective_action
        self.severity = severity

    def __str__(self) -> str:
        return str(self.__dict__)

    @staticmethod
    def get_rule(obj:Dict[str,str]):
        """

        :param obj: Dict[str,str]

        obj should contain:

        ========== ==========================================================================
        key        description
        ========== ==========================================================================
        rule_name  unique name within the set of rules, conventionally all caps
        table_name the name of the database table with the primary key
                   being returned from the sql
        msg        a brief description of the rule
        format     a str with embedded %s, as many as there are columns in the select result
        ========== ==========================================================================

        :return:
        """
        return ConditionIdentificationRule(obj["rule_name"],
                                           obj["table_name"],
                                           obj["msg"],
                                           obj["sql"],
                                           obj["format"])
