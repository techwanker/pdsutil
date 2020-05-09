========================
Condition Identification
========================

    Runs a series of sql statements that return as the first column, the
    primary key of a table being examined an 0 or more fields used in formatting
    a message.


    The result may be stored in UT_TABLE_ROW_MSG or the results just retrieved

    This may be used to find records that have error conditions or multi-record
    referential integrity problems.

    In combination with computed metrics outliers can be found

    .. glossary::

       rule_name
            Identifies the qualifying data in a succinct manner.
            Style wise this should be all caps e.g. **CUSTOMERS_WITH_NO_SALES**

       table_name
            The name of the database table from which the primary key is selected

       sql_text
            A select statement that returns as the first column the primary key, which
            must be an integer or numeric value with no scale.

            In postgres this would typically be a column declared as *serial*
            Other columns may be selected and can be used in conjunction with the
            *format_str* to generate a textual message

       narrative
            A clear description of the sql statement, the condition being qualified

       severity
            A number from 0 - 9, inclusive with 0 being low priority

            This can be used in subsequent processes to either halt processing or require approval

       format_str
            A python format string, each *%s* is replaced by the corresponding column in the select
            result.

            The number of columns in the select must match the number of *%s* in the format_str

    Each invocation starts a new run.

       * A row is inserted into ut_condition_run
       * The bind variables are inserted into ut_condition_run_parm
       * for each rule
         * The rule is retrieved from ut_condition and created if necessary.
         * For each row returned
           * a row is inserted into ut_condition_row_msg
         *  a row is inserted into ut_condition_run_step

    I generally externalize these rules in a .yaml file an example of which is::


        -   rule_name:  NO_CUSTOMER_TOTAL
            table_name: ETL_FILE
            msg:      No customer total record
            sql_text: >
                select etl_file_id
                from etl_file
                where etl_file_id = %(ETL_FILE_ID)s
                and not exists (
                   select 'x'
                   from etl_customer_tot
                   where etl_file_id = %(ETL_FILE_ID)s
                )
            narrative: >
                    There is no customer total record
            severity: 3
            format_str: "No customer total record in %s"

        -   rule_name: NO_SALES_FOR_CUSTOMER
            table_name: ETL_CUSTOMER
            msg:      No sales for customer
            sql_text: >
                select ec.etl_customer_id, ship_to_cust_id,
                     ec.cust_nm, etl_file_id
                from etl_customer ec
                where ec.etl_file_id = %(ETL_FILE_ID)s
                    and not exists (
                       select 'x'
                       from etl_sale
                       where etl_sale.etl_file_id = %(ETL_FILE_ID)s and
                         e c.ship_to_cust_id = etl_sale.ship_to_cust_id
                )
            severity: 1
            format_str: >
                etl_customer_id %s ship_to_cust_id %s name: %s has no sales in load %s

--------------------------------------------
Differences from javautil.org implementation
--------------------------------------------
    This version is subset of the javautil.org ConditionIdentification project and does not support:

    * Multiple threads for executing rules
    * Max elapsed time for query execution
    * Database statistics gathering for query execution
    * SQL plans
    * process logging other than through the ut_condition% tables and logger files (no process logging in the database)

    Queries should be written with binds as %(BIND_NAME)s, CursorHelper will run the statements and automatically
    convert the bind placeholders in the SQL with :BIND_NAME variables based on the inferred dialect which is
    deduced by the *python* type of the cursor.

-------------
Example Usage
-------------

.# Create a list of rules per the above format example


-------
History
-------
Condition Identication was written by Jim Schmidt in java for examing and reporting on feeds from distribution systems
for Trinity Technical Services Statistical Forecasting and Advanced Inventory Planning.

It was incorporated into Custom Data Solution processes while contracted to write a new version of vend processing.

This was subsequently used to replace the paper reporting system, which resulted in enormous operational efficiency
improvements and Custom Data Solutions.

