#!/usr/bin/env python
import yaml
import sys
import json
import pdsutil.JsonEncoder as JsonEncoder
import datetime
import logging

logger = logging.getLogger(__name__)


def load_file(file_name):
    """
    Reads the specified file and returns the object representation
    :param file_name:
    :return:
    """
    return load_yaml_file(file_name)


def load_yaml_file(file_name):
    """
    Reads the specified YAML file and returns the object representation
    #TODO put in util
    :param file_name:
    :return: The object representation of the YAML file
    """
    start_time = datetime.datetime.now()
    with open(file_name, 'r') as workbook_def_file:
        yaml_text = workbook_def_file.read()
    retval = yaml.load(yaml_text)
    end_time = datetime.datetime.now()
    elapsed = end_time - start_time
    logger.info("parse time for yaml %s" % elapsed)
    return retval


def get_pretty(obj):
    return json.dumps(obj, indent=2, sort_keys=False, cls=JsonEncoder)


if __name__ == "__main__":
    OBJ = load_file(sys.argv[1])
    print(get_pretty(OBJ))
