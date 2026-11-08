import copy
import re
import unittest
from policy import compile_rules


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.config={'brokerUser':'broker','grants':[{'user':'alice@example.com','catalog':'lake','schema':'public','table':'orders','rowFilter':"region = 'US'",'columns':[{'name':'email','mask':'CAST(NULL AS VARCHAR)'}]}]}

    def test_exact_identity_and_no_implicit_authority(self):
        rules=compile_rules(self.config)
        self.assertEqual(rules['impersonation'],[{'original_user':'^broker$','new_user':r'^alice@example\\.com\$','allow':True}])
        self.assertEqual(rules['queries'],[{'user':r'^alice@example\.com$','allow':['execute']}])
        self.assertEqual(rules['tables'][0]['privileges'],['SELECT'])
        self.assertEqual(rules['tables'][0]['filter'],"region = 'US'")
        for key in ('schemas','functions','procedures','system_session_properties','catalog_session_properties','system_information','authorization'):
            self.assertEqual(rules[key],[])
        self.assertEqual(rules['catalogs'][-1],{'catalog':'.*','allow':'none'})

    def test_duplicate_or_wildcard_grants_rejected(self):
        for patch in ({'user':'.*'},{'table':'orders|secrets'},{'catalog':'system'},{'user':'broker'},{'mask_environment':{'user':'admin'}}):
            c=copy.deepcopy(self.config);c['grants'][0].update(patch)
            with self.assertRaises(ValueError):compile_rules(c)
        c=copy.deepcopy(self.config);c['grants']*=2
        with self.assertRaises(ValueError):compile_rules(c)

    def test_empty_grants_revoke_all_authority(self):
        rules=compile_rules({'brokerUser':'broker','grants':[]})
        self.assertEqual(rules['tables'],[])
        self.assertEqual(rules['impersonation'],[])
        self.assertEqual(rules['queries'],[])
        self.assertEqual(rules['catalogs'],[{'catalog':'.*','allow':'none'}])

    def test_no_mask_environment_or_unknown_column_policy(self):
        c=copy.deepcopy(self.config);c['grants'][0]['columns'][0]['mask_environment']={'user':'admin'}
        with self.assertRaises(ValueError):compile_rules(c)

    def test_no_schema_or_write_permission_can_be_requested(self):
        for extra in ('privileges','owner','roles','grantSelect'):
            c=copy.deepcopy(self.config);c['grants'][0][extra]=True
            with self.assertRaises(ValueError):compile_rules(c)

    def test_publisher_exact_target_metadata_and_no_broker_impersonation(self):
        self.config['publishers']=[{'user':'publisher.service','catalog':'lake','schema':'public','table':'orders'}]
        rules=compile_rules(self.config)
        published=[r for r in rules['tables'] if re.fullmatch(r['user'],'publisher.service')]
        def privileges(name):
            return [r['privileges'] for r in published if re.fullmatch(r['table'], name)]
        self.assertEqual(privileges('orders'), [['SELECT','OWNERSHIP']])
        self.assertEqual(privileges('orders$properties'), [['SELECT']])
        self.assertEqual(privileges('orders$history'), [['SELECT']])
        for name in ('other','orders$partitions','ordersXproperties','orders$history_suffix'):
            self.assertEqual(privileges(name), [])
        self.assertEqual(rules['procedures'], [{'user':r'^publisher\.service$','catalog':'^lake$','schema':'^system$','procedure':'^register_table$','privileges':['EXECUTE']}])
        self.assertEqual(rules['schemas'], [])
        self.assertEqual(len(rules['impersonation']), 1)
        self.assertNotIn('publisher', rules['impersonation'][0]['new_user'])
        reader=rules['tables'][0]
        self.assertEqual(reader['privileges'], ['SELECT'])
        self.assertEqual(reader['filter'], "region = 'US'")
        self.assertFalse(re.fullmatch(reader['table'], 'orders$properties'))
        self.assertIn({'user':r'^publisher\.service$','catalog':'^lake$','allow':'all'}, rules['catalogs'])

    def test_publisher_rejects_identity_overlap_wildcards_duplicates_and_extra_authority(self):
        baseline={'user':'publisher','catalog':'lake','schema':'public','table':'orders'}
        for patch in ({'user':'broker'},{'user':'alice@example.com'},{'catalog':'system'},
                      {'schema':'information_schema'},{'table':'orders.*'},
                      {'procedure':'drop_stats'},{'rowFilter':'true'},{'user':None}):
            with self.subTest(patch=patch):
                config=copy.deepcopy(self.config)
                config['publishers']=[baseline | patch]
                with self.assertRaises(ValueError):compile_rules(config)
        for publishers in (None, {}, [baseline]*2, [baseline]*1001):
            with self.assertRaises(ValueError):compile_rules(self.config | {'publishers':publishers})

    def test_empty_readers_do_not_make_publisher_impersonable(self):
        rules=compile_rules({'brokerUser':'broker','grants':[], 'publishers':[
            {'user':'publisher','catalog':'lake','schema':'public','table':'orders'}]})
        self.assertEqual(rules['impersonation'], [])
        self.assertEqual(rules['queries'], [{'user':'^publisher$','allow':['execute']}])
