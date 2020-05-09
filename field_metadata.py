# from datetime import datetime, date
import datetime
import decimal
# from datetime import datetime
# http://stackoverflow.com/questions/15707532/python-import-datetime-v-s-from-datetime-import-datetime
from decimal import *
import logging

NUMERIC = "NUMERIC"
INTEGER = "INTEGER"
DECIMAL = "DECIMAL"
DATE = "DATE"
TEXT = "TEXT"
LITERAL = "LITERAL"
DIGITS = "DIGITS"

logger = logging.getLogger(__name__)

def to_formatted_string(fld_def, p_obj, trace:bool=False):
    obj = p_obj
    # TODO logging print " obj is %s %s" % (obj, type(obj))
    if trace:
       print ("to_formatted_string_meta obj %s type %s fld_def %s" % (obj, type(obj), fld_def))

    if fld_def["field_type"] == TEXT:
        if p_obj is None:
            p_obj = ""

    if "str_format" not in fld_def:
        raise Exception("No format string in %s" % (fld_def))
    str_format = fld_def["str_format"]

    if fld_def["field_type"] == LITERAL:
        logger.debug("str_format: '%s' for field %s" % (fld_def["str_format"], fld_def["field_name"]))
        retval = fld_def["str_format"].format(fld_def["fixed_literal"])  # TODO doesn't apply formatter
    else:
        if trace:
            print("obj %s type %s format %s " % (str(obj), str(type(obj)),fld_def["str_format"]))
        if isinstance(obj, str):
            retval = str_format.format(obj)
        elif isinstance(obj, datetime.date):
            retval = obj.strftime(fld_def["str_format"])
            if trace:
                print ("obj %s format %s retval %s" % (obj, fld_def["str_format"], retval))
        elif isinstance(obj, decimal.Decimal):
            try:
                retval = str_format.format(obj)
                logger.debug("formatted Decimal %s to '%s' using '%s'" % (obj, retval, str_format))
            except Exception as e:
                message = "obj %s, obj type %s,  fld_def %s, exception %s" % (obj, type(obj), fld_def, str(e))
                raise Exception(message)
        elif isinstance(obj, int):
            try:
                retval = str_format.format(obj)
                logger.debug("formatted int %s to '%s' using '%s'" % (obj, retval, str_format))
            except Exception as e:
                raise Exception("obj %s fld %s %s" % (obj, fld_def, str(e)))
        elif fld_def["field_type"] == "TEXT" and obj is None:
            retval = str_format.format("")
        else:
            # print fld_def  # TODO fix execption
            # print obj
            raise Exception("Unsupported type  obj:%s\nfield_type:%s\nfield_name: %s\ndef %s " %
                            (type(obj), fld_def["field_type"], fld_def["field_name"], fld_def))
    return retval


def to_object(meta, text):  # TODO make these module level and call from class methods

    field_type = meta["field_type"]
    logger.debug("to_object_meta text: %s, type text %s, meta %s" % (text, type(text), meta))
    if field_type == NUMERIC:  #TODO where is this used
        retval = Decimal(text)  # make sure is numeric
        retval = int(text)  # TODO create an integer field
    elif field_type == TEXT:
        retval = str(text).rstrip()
    elif field_type == DATE:  # TODO must support format fields
        dt = datetime.datetime.strptime(text, "%Y%m%d")
        retval = dt
    elif field_type == LITERAL:
        retval = text
    elif field_type == DIGITS:
        retval = text
    elif field_type == INTEGER:
        retval = int(text)
    else:  # will blowup if not valid
        raise Exception("unhandled field type %s" % field_type)
    # logger.debug("field_name %s, field_type %s text '%s' obj '%s' type %s" %
    # (meta["field_name"], field_type, text, retval, type(retval)))
    return retval


def get_field_dictionary(fields):
    field_dict = {}
    for field in fields:
        field_dict[field["field_name"]] = field
    return field_dict


def get_field_obj(field_metadata, record):
    """
    Returns an object representation of the specified field in the fixed length
    record as specified in config/cd
    #TODO could make this generic by passing the fixed_offset and length, field_type and date parsing format
    :param field_metadata:
    :param record:
    :return:
    """
    start_pos = field_metadata["fixed_offset"]
    length = field_metadata["fixed_length"]
    end_pos = start_pos + length
    text = record[start_pos:end_pos]
    return to_object(field_metadata, text)


def format_line(record_definition, field_data_map,trace=False):
    """
    Creates a record of fixed length with fixed length fields
    field_data_map must have the attributes length and offset

    :param record_definition a list of FieldMetadata at least supports __get_item__
    :param field_data_map:
    :return:
    """
    if trace:
        print ("format_line trace is %s" % trace)
    # TODO validate record_definition
    # todo what are fields field_data_map should be binds
    # record definition is list of FieldMetadatinfo
    # logger.debug(" field_data_map: %s \n  record_definition: %s" % (field_data_map, record_definition)S
    buffer = []
    ndx = 0
    for field_def in record_definition:

        field_name = field_def["field_name"]
        if field_name not in field_data_map:
            message = "field %s not found in \n %s" % (field_name, field_data_map)
            raise Exception(message)
        field_data = field_data_map[field_def["field_name"]]
        formatted_field = to_formatted_string(field_def, field_data,trace)

        if trace:
            print("field_name %s, field_data '%s' , type %s formatted '%s' len %s" %
                     (field_def["field_name"], field_data, type(field_data), formatted_field, len(formatted_field)))

        try:
            start_pos = field_def["fixed_offset"]
            length = field_def["fixed_length"]
            buffer[start_pos:start_pos + length] = formatted_field
            if trace:
                print("start_pos %s length %s formatted '%s'" % (start_pos, length, formatted_field))
                print("".join(buffer))
        except Exception as e:
            raise Exception(
                "Processing rec_def %s fld %s adding %s and got %s" % (field_def, field_data, formatted_field, e))

        ndx += 1
    retval = "".join(buffer)
    return retval


# def getAsDictionary(field_list):  #TODO improper name
#     dictionary = dict()
#     for field in field_list:  # TODO could probably be done pythonic
#         dictionary[field.field_name] = field.getMetaDictionary()
#     return dictionary


def get_bind_map(field_defs, inrec):
    """

    :param field_defs:
    :param inrec:
    :return:
    """
    binds = {}

    for field_def in field_defs:
        try:
            value = get_field_obj(field_def, inrec)
            logging.debug("field_def %s, value %s" % (field_def, value))
            binds[field_def["field_name"]] = value
        except Exception as e:
            text = "While processing \n {}\n {}\n {}\n".format(inrec, field_def, e)
            raise Exception(text)
    return binds
