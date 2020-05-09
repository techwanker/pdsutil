
def ensure(parm_name, parm, parmtype, required=False):
    if parm is None:
        if required:
             raise Exception("required parameter %s is None" % parm_name)
    else:
        if not isinstance(parm, parmtype):
            raise Exception("parm : %s value %s is type %s not %s" % (parm_name, parm, type(parm), parmtype))
