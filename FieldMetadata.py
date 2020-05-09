# from datetime import datetime, date
import datetime
# from datetime import datetime

# http://stackoverflow.com/questions/15707532/python-import-datetime-v-s-from-datetime-import-datetime

from decimal import *

NUMERIC = "NUMERIC"
INTEGER = "INTEGER"
DECIMAL = "DECIMAL"
DATE = "DATE"
TEXT = "TEXT"
LITERAL = "LITERAL"




class FieldMetadata:
    # Format Strings
    YYYYMMDD = "%Y%m%d"

    def __init__(self,
                 field_name,
                 field_type,
                 length=None,  # used when parsing a record
                 offset=None,
                 parse_format=None,
                 str_format=None,
                 literal=None):
        """

        :param field_name: - str This should be the database column name
        :param length: -
        :param field_type:
        :param offset:
        :param parse_format:
        :param str_format:
        :param literal:
        """
        self.offset = offset
        self.length = length
        self.field_type = field_type
        self.field_name = field_name
        self.parse_format = parse_format
        self.str_format = str_format
        self.literal = literal
        # print self
        if field_type == LITERAL:
            if literal is None:
                raise Exception("literal is required for type LITERAL field_name %s")

            if not len(literal) == length:
                print ("type length %s") % type(length)
                raise Exception("literal '%s' is length %s and %s is required" % (literal, len(literal), length))

    def to_formatted_string(self, obj):
        # TODO logging print " obj is %s %s" % (obj, type(obj))
        if self.field_type == self.LITERAL:
            retval = self.literal
        else:
            if self.str_format is None:
                raise Exception("No format string in %s" % self)
            if isinstance(obj, str):
                retval = self.str_format.format(obj)
                # TODO logging print ("formatted str '%s' with '%s' resulting in '%s'" % (obj, self.str_format, retval))
            elif isinstance(obj, datetime.datetime):
                retval = obj.strftime(self.str_format)
                print ("emitting %s formatted with %s as %s" % (obj, self.str_format, retval))
            elif isinstance(obj, int):
                try:
                    retval = self.str_format.format(obj)
                except Exception as e:
                    raise Exception("obj %s fld %s :%s" % (obj, self, str(e)))
            else:
                print (self)  # TODO fix execption
                print (obj)
                raise Exception("Unsupported type %s %s name %s" % (type(obj), self.field_type, self.field_name))

        return retval

    def to_object(self, text):
        if self.field_type == NUMERIC:
            retval = Decimal(text)  # make sure is numeric
            retval = int(text)  # TODO create an integer field
        elif self.field_type == TEXT:
            retval = str(text).rstrip()
        elif self.field_type == DATE:  # TODO must support format fields
            # TODO logging print "processing %s with '%s' " % (self, text)
            dt = datetime.datetime.strptime(text, "%Y%m%d")
            retval = dt
        elif self.field_type == LITERAL:
            retval = text
        else:  # will blowup if not valid
            raise Exception("unhandled field type")
        return retval

    def get_field(self, record):
        text = record[self.offset:self.offset + self.length]
        # logging.debug("text for %s is '%s'" % (self, text))
        return self.to_object(text)


    def __str__(self):  # TODO use dict to report or add other fields
        """
        returns dictionary of attributes as a string

        """
        return ("field_name: '%s', field_type: '%s', length: %s, offset: %s, "
                "parse_format: '%s', str_format: '%s', literal: '%s'" %
                (self.field_name, self.field_type, self.length, self.offset,
                 self.parse_format, self.str_format, self.literal)
                )

    def getMetaDictionary(self):
        meta = {
            "field_name": self.field_name,
            "field_type": self.field_type,
            "offset": self.offset,
            "length": self.length,
        }
        return meta

    @staticmethod
    def getAsDictionary(field_list):
        dictionary = dict()
        for field in field_list:  # TODO could probably be done pythonic
            dictionary[field.field_name] = field.getMetaDictionary()
        return dictionary


