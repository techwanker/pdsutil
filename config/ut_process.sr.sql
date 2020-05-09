create table ut_process_status (
  ut_process_status_id   serial primary key,
  schema_nm              varchar(30),
  process_nm             varchar(128),
  thread_nm              varchar(128),
  process_run_nbr        numeric(9),
  status_msg             varchar(256),
  status_id              varchar(1),
  status_ts              timestamp(6),
  --total_elapsed          interval day(2) to second(6),
  --sid                    numeric,
  --serial#                numeric,
  ignore_flg             varchar(1)       default 'N'
)

--alter table ut_process_status add (
--  check ( ignore_flg in ('Y', 'N')))

;--
create table ut_process_log (
  ut_process_log_id     serial primary key,
  ut_process_status_id  integer references
    ut_process_status(ut_process_status_id),
  log_seq_nbr            numeric(18)             not null,
  log_msg_id             varchar(8),
  log_msg                text,
  log_msg_ts             timestamp(6),
  --elapsed_time           interval day(2) to second(6),
  --
  caller_name            varchar(100),
  line_nbr               numeric(5),
  call_stack             text,
  log_level              numeric(2)
)
;--


