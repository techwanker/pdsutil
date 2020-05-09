from collections import OrderedDict
import logging
NAME = "name"
DISPLAY_LENGTH = "display_length"
HEADING = "heading"
FORMAT_STR = "format_str"
known_keys = [DISPLAY_LENGTH, HEADING, FORMAT_STR, NAME]

class WorksheetColumnMeta:


    def __init__(self,**kwargs):
        """

        :param column_name: the name of the column as corresponds to names from data
        :param heading: the heading to be applied above the data in the spreadsheet
        :param width: column width
        :param format_str: excel format string for data
        """

        logger = logging.getLogger(__name__)
        logger.info("instantiating %s" % kwargs)
        self.column_name = kwargs[NAME]
        if HEADING in kwargs:
            self.heading = kwargs[HEADING]
        else:
            self.heading = self.column_name
        if DISPLAY_LENGTH in kwargs:
            self.width = kwargs[DISPLAY_LENGTH]
        else:
            self.width = None
        if FORMAT_STR in kwargs:
            self.format_str = kwargs[FORMAT_STR]
        else:
            self.format_str = None
        for k in kwargs:
            if k not in known_keys:
                raise Exception("unknown parameter %s " % k)

        logger.info("instantiated %s" % str(self))

    def merge(self,other):
        """
        Returns a new instance, applies self first
        :param other:
        :return:
        """
        retval = WorksheetColumnMeta(self.column_name, self.heading, self.format_str, self.width)
        if not self.column_name == other.column_name:
            raise Exception("merge exception, different names, self: %s other: %s" % self.column_name, other.column_name)
        if other.heading is not None:
            retval.heading = other.heading
        if other.width is not None:
            retval.width = other.width
        if other.format_str is not None:
            retval.format_str = other.format_str
        return retval

    def __str__(self):
        return ("column_name: %s heading: %s, display_length %s, format_str %s"
                  % (self.column_name, self.heading, self.width, self.format_str))


class WorksheetMeta:
    """
    Contains metadata on columns
    """
    logger = logging.getLogger(__name__)
    def __init__(self):
        self.columns = OrderedDict()

    def add_column(self, column_meta):
        """
        :param column_meta: WorksheetColumnMeta
        :return: None
        """
        self.logger.info("adding column %s" % (str(column_meta)))
        self.logger.debug("type columns %s" % type(self.columns))
        column_name = column_meta.column_name
        self.columns[column_name] = column_meta

    def add_column_dict(self,column_meta_dict):
        """
        :param column_meta: WorksheetColumnMeta
        :return: None
        """
        self.logger.info("adding column from dictionary %s" % (str(column_meta_dict)))
        self.logger.debug("type columns %s" % type(self.columns))
        # column_name = column_meta_dict["name"]
        column_meta = WorksheetColumnMeta(**column_meta_dict)
        self.columns[column_meta.column_name] = column_meta

    def merge(self,other):
        """
        Creates a new instance by copying this and applying updates from
        other
        :param other:
        :return:
        """
        retval = WorksheetMeta()
        for colname, colmeta in self.columns.items():
            retval.add_column(colmeta)
        for other_colname, other_colmeta in other.columns.items():
            if colname in retval.columns:
                merged = retval.columns[colname].merge(other_colmeta)
            else:
                retval.add_column(other_colmeta)
        return retval


    def __str__(self):
        retval = ""
        for colname, colmeta in self.columns.items():
            retval += str(colmeta)
            retval += "\n"
        return retval

