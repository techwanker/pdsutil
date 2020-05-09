
import pdsutil.address_information as address_information
import unittest

class testAddressInformation(unittest.TestCase):

    def test_burney(self):
        addr = dict([
            ('address', '1095 Berney Lane'),
            ('city', 'Southlake'),
            ('state', 'TX'),
        ])

        response = address_information.verify('714PACIF0190', addr)

        print (response)

        self.assertEquals(response['address'], '1095 BURNEY LN')
        self.assertEquals(response['city'], 'SOUTHLAKE')
        self.assertEquals(response['state'], 'TX')
        self.assertEquals(response['zip5'], '76092')




    # def test_mutiple_address_verify():
    #
    #
    #     addrs = [
    #         dict([
    #             ('address', '6406 Ivy Lane'),
    #             ('city', 'Greenbelt'),
    #             ('state', 'MD'),
    #         ]),
    #         dict([
    #             ('address', '8 Wildwood Drive'),
    #             ('city', 'Old Lyme'),
    #             ('state', 'CT'),
    #         ]),
    #     ]
    #     address_information.verify('foo_id', *addrs)
    #     [
    #         dict([
    #             ('address', '6406 IVY LN'),
    #             ('city', 'GREENBELT'),
    #             ('state', 'MD'),
    #             ('zip5', '20770'),
    #             ('zip4', '1441'),
    #         ]),
    #         dict([
    #             ('address', '8 WILDWOOD DR'),
    #             ('city', 'OLD LYME'),
    #             ('state', 'CT'),
    #             ('zip5', '06371'),
    #             ('zip4', '1844'),
    #         ]),
    #    ]