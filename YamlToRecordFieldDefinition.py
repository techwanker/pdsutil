from typing import List, Dict
#from pdsutil.FieldMetadata import FieldMetadata

def in_list(value: str, p_list: List[str]):
    return value in p_list


def in_dict(value: str, p_dict: Dict[str, str]):
    return value in p_dict


# class YamlToRecordFieldDefinition:
#     FIELD_NAME = "field_name"
#     FIELD_TYPE = "field_type"
#     PARSE_FORMAT = "parse_format"
#     STR_FORMAT = "str_format"
#     LITERAL = "literal"
#     LENGTH = "length"
#     OFFSET = "offset"
#
#     """
#     TODO, this can be fleshed out and used to validate a YAML
#     """
#     def get_record_defs(self, record_dict:List[Dict[str,str]]):
#             """
#             Converts a dictionary as returned from parsing YAML into a list of FieldDefinition
#             :param record_dict:
#             :return:
#             """
#             print ("in get_record_defs %s " % record_dict)
#             print ("type record_dict %s " % type(record_dict))
#             retval = {}
#             for k, rec_def in record_dict.iteritems():
#                 print ("k %s %s " % (k, rec_def))
#                 fields = []
#                 retval[k] = fields
#                 for fld_def in rec_def:
#                     field_name = fld_def.get("field_name"),
#                     field_type = fld_def.get("field_type"),
#                     parse_format = fld_def.get("parse_format", None),
#                     str_format = fld_def.get("str_format", None),
#                     literal = fld_def.get("literal", None)
#                     length = fld_def.get(YamlToRecordFieldDefinition.LENGTH, None),  # TODO should have Record
#                     offset = fld_def.get("offset", None)
#                     print ("name: %s: value: %s, ")
#                     if length is not None:
#                         print ("length is %s type %s" % (length, type(length)))
#                         length = int(length)
#                     offset = fld_def.get("offset", None)
#                     if offset is not None:
#                         offset = int(offset)
#                     meta = FieldMetadata(
#                         fld_def.get("field_name"),
#                         fld_def.get("field_type"),
#                         length,
#                         offset,
#                         parse_format=fld_def.get("parse_format", None),
#                         str_format=fld_def.get("str_format", None),
#                         literal=fld_def.get("literal", None)
#                     )
#                     fields.append(meta)
#             return retval
