from pdsutil.DbUtil import CursorHelper
class UtProcess:
    def __init__(self,connection):
        self.connection = connection
        self.process_cursor = CursorHelper(connection.cursor())
        self.step_cursor = CursorHelper(connection.cursor())

    def insert_process(self, binds):
        insert_sql = """
        insert into ut_process_status (
            schema_nm,
            process_nm,
            thread_nm,
            process_run_nbr,
            status_msg,
            status_id,
            status_ts,
            ignore_flg
        ) values (
            %(schema_nm)s,
            %(process_nm)s,
            %(thread_nm)s,
            %(process_run_nbr)s,
            %(status_msg)s,
            %(status_id)s,
            %(status_ts)s,
            %(ignore_flg)s
        ) """
        self.process_cursor.execute(insert_sql,binds)

    def get_process_for_id(self,id):
        sql = "select * from ut_process where ut_process_id"

    def insert_process_step(self,binds):
        sql = """
        insert into ut_process_log (
          ut_process_status_id,
          log_seq_nbr,
          log_msg_id,
          log_msg,
          log_msg_ts,
          caller_name,
          line_nbr,
          call_stack,
          log_level
        ) values
        (
          %(ut_process_status_id)s,
          %(log_seq_nbr)s,
          %(log_msg_id)s,
          %(log_msg)s,
          %(log_msg_ts)s,
          %(caller_name)s,
          %(line_nbr)s,
          %(call_stack)s,
          %(log_level)s
        )
        """
        self.step_cursor.execute(sql, binds)
