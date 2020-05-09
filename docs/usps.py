from pdsutil.address_information import address_information

addrs = [
    dict([
            ('address', '6406 Ivy Lane'),
            ('city', 'Greenbelt'),
            ('state', 'MD'),
            ]),
    dict([
            ('address', '8 Wildwood Drive'),
            ('city', 'Old Lyme'),
            ('state', 'NJ'),
            ]),
   ]
address_information.verify('foo_id', *addrs)

# dict([
#     ('address', '6406 IVY LN'),
#     ('city', 'GREENBELT'),
#     ('state', 'MD'),
#     ('zip5', '20770'),
#     ('zip4', '1441'),
#     ])



addr = dict([
     ('address', '6406 Ivy Lane'),
     ('city', 'Greenbelt'),
     ('state', 'MD'),
     ])
address_information.verify('foo_id', addr)
# dict([
#     ('address', '6406 IVY LN'),
#     ('city', 'GREENBELT'),
#     ('state', 'MD'),
#     ('zip5', '20770'),
#     ('zip4', '1441'),
#     ])