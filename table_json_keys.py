# table_json_keys.py
COLUMNS="columns"
TABLE_NAME="TABLE_NAME"
PRIMARY_KEY_COLUMN="pkColumn"
ENTITY_NAME="entityName"
JAVA_TYPE="javaType"        # as in oracle dba_tab_columns, all_tab_columns, user_tab_columns
DATA_TYPE="DATA_TYPE"      # as in oracle dba_tab_columns, all_tab_columns, user_tab_columns
NULLABLE="NULLABLE"         # as in oracle dba_tab_columns, all_tab_columns, user_tab_columns
DATA_LENGTH="DATA_LENGTH"     # as in oracle dba_tab_columns, all_tab_columns, user_tab_columns
JAVA_ATTRIBUTE_NAME="javaAttributeName"  # the name with every character after an underscore as uppercase,
                                         # initial lowercase and underscores removed
PYTHON_MEMBER_NAME="python_member_name"  # COLUMN_NAME all lower case, retaining underscores
MODEL_TYPE  = "model_type"    # django model type in models.py
COLUMN_NAME = "COLUMN_NAME"   # as in oracle dba_tab_columns, all_tab_columns, user_tab_columns
