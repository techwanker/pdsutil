
import datetime
import json
import logging

import pdsutil.ConditionIdentificationRule as ConditionIdentificationRule
import pdsutil.parm_util as parm_util
from pdsutil.DbUtil import Cursors
from pdsutil.JsonEncoder import JsonEncoder
from typing import Dict

logger = logging.getLogger(__name__)


class UtConditionPersistence:
    """
       DML methods for ut_condition
    """

    """
    create table ut_condition (
           ut_condition_id         serial primary key,
           condition_name          varchar(30) not null,
           table_name              varchar(60) not null,
           condition_msg           text,
           sql_text                text  not null,
           narrative               text,
           condition_severity      numeric(1),
           condition_format_str    text,
           corrective_action       text
    )
    """

    def clean_run(self,connection):
        "delete from ut_table_row_msg "


    @staticmethod
    def select_exact(cursors: Cursors, condition:ConditionIdentificationRule ) -> int:
        """

        :param cursors: a pdsutil.Cursors
        :param condition: pdsutil.ConditionIdentificationRule
        :return: the primary key of the existing or created ut_condition record
        """

        """
            Returns the primary key of a row in ut_condition

            If a row is not in the table that matches the condition
        """
        sql = """
            select ut_condition_id
            from ut_condition
            where
                condition_name          =         %(rule_name)s and
                table_name              =         %(table_name)s and
                condition_msg           =         %(msg)s and
                sql_text                =         %(sql_text)s and
                (narrative              =         %(narrative)s or
                 narrative is null and %(narrative)s is null) and
                condition_severity      =         %(severity)s and
                condition_format_str    =         %(format_str)s """
        """
                --and
                --corrective_action       =         %(corrective_action)s"""

        cur = cursors.get_cursor(sql)
        if "narrative" not in condition:
            condition["narrative"] = None
        rows = cur.execute(sql, condition)

        retval = None
        for row in rows:
            retval = row[0]
        return retval

    @staticmethod
    def insert(cursors: Cursors, condition:ConditionIdentificationRule) -> None:
        """
        inserts into ut_condition unless there is an exact match
        in which case it returns the matching record id
        """

        ut_condition_insert = """
        insert into ut_condition (
            condition_name,
            table_name,
            condition_msg,
            sql_text,
            narrative,
            condition_severity,
            condition_format_str
        ) values (
            %(rule_name)s,
            %(table_name)s,
            %(msg)s,
            %(sql_text)s,
            %(narrative)s,
            %(severity)s,
            %(format_str)s
        )
        """

        cur = cursors.get_cursor(ut_condition_insert)
        if 'narrative' not in condition:
            condition['narrative'] = None
        retval = cur.execute(ut_condition_insert, condition,returning="returning ut_condition_id")
        return retval
    @staticmethod
    def fetch_or_insert(cursors:Cursors, binds:dict) -> None:
        """
        
        :param cursors: 
        :param binds: 
        
        :return: 
        
        Binds:
            rule_name,
            table_name,
            msg,
            sql_text,
            narrative,
            severity,
            format_str
        
        """
        retval = UtConditionPersistence.select_exact(cursors, binds)
        if retval is None:
            retval = UtConditionPersistence.insert(cursors, binds)
        return retval


class UtConditionRunPersistence:
    @staticmethod
    def start_run(cursors:Cursors, binds:Dict[str,object]) -> int:
        """
        :param cursors - Cursors from
        :param binds: dictionary of bind variables for condition statements
        :return: ut_condition_run_id
        """
        pretty_binds = json.dumps(binds, indent=4, cls=JsonEncoder)
        logger.info("starting run with binds %s" % pretty_binds)
        sql = "insert into ut_condition_run (" \
              "   start_ts" \
              ") values (" \
              "   %(start_ts)s" \
              ")"
        returning = "returning ut_condition_run_id"

        cursor = cursors.get_cursor(sql)
        now = datetime.datetime.now()
        retval = cursor.execute(sql, {"start_ts": now}, returning=returning,verbose=True)
        # retval = None
        # for row in rows:
        #     retval = [0]

        parms_sql = """
        insert into ut_condition_run_parm (
            ut_condition_run_id, parm_nm, parm_type, parm_value_str
        ) values (
           %(UT_CONDITION_RUN_ID)s, %(PARM_NM)s, %(PARM_TYPE)s, %(PARM_VALUE)s
        )"""
        parms_cursor = cursors.get_cursor(parms_sql)
        for k, v in binds.items():
            print ("k: %s v: %s type v %s" % (k, v, type(v)))
            parms = {"UT_CONDITION_RUN_ID": retval, "PARM_NM": k,
                     "PARM_TYPE": str(type(v)), "PARM_VALUE": str(v)}
            parms_cursor.execute(parms_sql, parms)
        return retval


class UtConditionRunStepPersistence:
    @staticmethod
    def insert(cursors:Cursors, run_id:int, start_ts:datetime.date, condition:ConditionIdentificationRule) -> int:
        """
        Inserts a row into ut_condition_run_step
        :param cursors Cursors
        :param run_id: int - ut_condition_run_id
        :param start_ts: datetime.date - timestamp start of test time
        :param condition: dict of current condition
        :return: ut_condition_run_step_id
        """
        ut_condition_id = UtConditionPersistence.fetch_or_insert(cursors, condition)
        sql = """
        insert into ut_condition_run_step (
            ut_condition_id,
            ut_condition_run_id,
            start_ts
        ) values (
            %(UT_CONDITION_ID)s,
            %(UT_CONDITION_RUN_ID)s,
            %(START_TS)s
        ) 
        """
        binds = {"UT_CONDITION_ID": ut_condition_id,
                 "UT_CONDITION_RUN_ID": run_id,
                 "START_TS": start_ts}
        returning = "returning ut_condition_run_step_id"
        cursor = cursors.get_cursor(sql)
        ut_condition_run_step_id = cursor.execute(sql, binds, returning=returning)
        return ut_condition_run_step_id


class UtConditionRowMsgPersistence:
    """
    create table ut_condition_row_msg (
    ut_condition_run_step_id integer references ut_condition_run_step,
    table_pk                 integer,
    condition_msg            varchar(200),
    primary key (ut_condition_run_step_id, table_pk)
    );


    """
    @staticmethod
    def insert(cursors:Cursors, ut_condition_run_step_id:int, primary_key:int, message:str) -> None:
        """
        Inserts a row into ut_condition_row_msg

        :param cursors:
        :param ut_condition_run_step_id:
        :param primary_key:
        :param message:

        :return:
        """


        parm_util.ensure("ut_condition_run_set_id", ut_condition_run_step_id, int)
        parm_util.ensure("primary_key", primary_key, int)

        parm_util.ensure("message", message, str)
        binds = {
            "UT_CONDITION_RUN_STEP_ID": ut_condition_run_step_id,
            "PRIMARY_KEY": primary_key,
            "MSG": message
        }

        ut_table_row_msg_sql = """
             insert into ut_condition_row_msg (
                 ut_condition_run_step_id,
                 table_pk,
                 condition_msg
             ) values (
                 %(UT_CONDITION_RUN_STEP_ID)s, %(PRIMARY_KEY)s,
                 %(MSG)s
             )
        """

        insert_cursor = cursors.get_cursor(ut_table_row_msg_sql)
        insert_cursor.execute(ut_table_row_msg_sql, binds)

