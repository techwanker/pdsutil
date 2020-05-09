import json
import datetime
from decimal import *


class JsonEncoder(json.JSONEncoder):  # TODO options for date format
    """
    Converts Decimal to float and datetime.date to isoformat string to allow serialization
    of these types
    """
    def default(self, obj:object):

        obj_type = str(type(obj))

        if isinstance(obj, Decimal):
            retval = float(obj)
        elif isinstance(obj,datetime.date):
            retval = obj.isoformat()
        else:
            retval = json.JSONEncoder.default(self, obj)
        return retval

# self.rules = json.loads(open(file_name).read().decode('utf-8-sig'))
