#!/usr/bin/env python3
"""Compile administrator-owned exact grants to Trino 484 file access rules.

The output is engine configuration, not a proof that the running engine loaded
it. Filters and masks are trusted administrator SQL; caller queries cannot set
them. Reader identities have no write or metadata grants. Optional publication
identities have ownership of exact targets and register_table access. They are
privileged service identities: target ownership can permit other DDL too.
"""
import argparse
import json
from pathlib import Path
import re


def exact(value, *, identity=False):
    pattern = r'[A-Za-z0-9][A-Za-z0-9_.:@-]{0,255}' if identity else r'[A-Za-z_][A-Za-z0-9_]{0,127}'
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError('Identity or SQL identifier is invalid')
    return '^' + re.escape(value) + '$'


def expression(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 16384 or '\x00' in value:
        raise ValueError('A nonempty bounded administrator SQL expression is required')
    return value


def compile_rules(config):
    if not isinstance(config, dict) or not {'brokerUser', 'grants'} <= set(config) or set(config)-{'brokerUser','grants','publishers'}:
        raise ValueError('Expected brokerUser, grants and optional publishers only')
    broker = exact(config['brokerUser'], identity=True)
    grants = config['grants']
    if not isinstance(grants, list) or len(grants) > 1000:
        raise ValueError('Expected at most 1000 exact grants; [] denies all')
    rules = {key: [] for key in ('catalogs','schemas','tables','functions','procedures',
        'system_session_properties','catalog_session_properties','queries','impersonation',
        'system_information','authorization')}
    users, catalogs, seen = set(), set(), set()
    for grant in grants:
        if not isinstance(grant, dict) or not {'user','catalog','schema','table'} <= set(grant) or set(grant)-{'user','catalog','schema','table','rowFilter','columns'}:
            raise ValueError('Grant has missing or unknown fields')
        user = exact(grant['user'], identity=True)
        catalog, schema, table = (exact(grant[k]) for k in ('catalog','schema','table'))
        if user == broker or grant['catalog'] == 'system' or grant['schema'] == 'information_schema':
            raise ValueError('Broker identity and engine system metadata cannot receive table grants')
        key = (user,catalog,schema,table)
        if key in seen:
            raise ValueError('Duplicate grants are ambiguous under first-match rules')
        seen.add(key); users.add(user); catalogs.add((user,catalog))
        item = {'user':user,'catalog':catalog,'schema':schema,'table':table,'privileges':['SELECT']}
        if 'rowFilter' in grant: item['filter'] = expression(grant['rowFilter'])
        if 'columns' in grant:
            columns = grant['columns']
            if not isinstance(columns,list) or len(columns)>1000:
                raise ValueError('Expected bounded column constraints')
            names=set(); item['columns']=[]
            for column in columns:
                if not isinstance(column,dict) or 'name' not in column or set(column)-{'name','allow','mask'}:
                    raise ValueError('Column constraints permit name, allow and mask only')
                exact(column['name'])
                if column['name'].lower() in names: raise ValueError('Duplicate column constraint')
                names.add(column['name'].lower())
                constraint={'name':column['name']}
                if 'allow' in column:
                    if not isinstance(column['allow'],bool): raise ValueError('allow must be boolean')
                    constraint['allow']=column['allow']
                if 'mask' in column: constraint['mask']=expression(column['mask'])
                item['columns'].append(constraint)
        rules['tables'].append(item)
    reader_users = set(users)
    publishers = config.get('publishers', [])
    if not isinstance(publishers, list) or len(publishers) > 1000:
        raise ValueError('Expected at most 1000 exact publication targets')
    publisher_catalogs, publisher_targets = set(), set()
    for publisher in publishers:
        if not isinstance(publisher, dict) or set(publisher) != {'user','catalog','schema','table'}:
            raise ValueError('Publisher requires exact user, catalog, schema and table only')
        user = exact(publisher['user'], identity=True)
        catalog, schema, table = (exact(publisher[k]) for k in ('catalog','schema','table'))
        if user == broker or user in reader_users or publisher['catalog'] == 'system' or publisher['schema'] == 'information_schema':
            raise ValueError('Publisher must be separate from broker and readers and cannot target system metadata')
        target = (user,catalog,schema,table)
        if target in publisher_targets:
            raise ValueError('Duplicate publication target')
        publisher_targets.add(target); publisher_catalogs.add((user,catalog)); users.add(user)
        rules['tables'].append({'user':user,'catalog':catalog,'schema':schema,'table':table,'privileges':['SELECT','OWNERSHIP']})
        # Readers never receive these metadata grants. Escape the suffix to
        # keep the dollar sign literal in Java regular expressions.
        for suffix in ('$properties', '$history'):
            rules['tables'].append({'user':user,'catalog':catalog,'schema':schema,'table':'^'+re.escape(publisher['table']+suffix)+'$','privileges':['SELECT']})
    rules['catalogs']=[{'user':user,'catalog':catalog,'allow':'read-only'} for user,catalog in sorted(catalogs)]
    rules['catalogs'] += [{'user':user,'catalog':catalog,'allow':'all'} for user,catalog in sorted(publisher_catalogs)]
    rules['procedures']=[{'user':user,'catalog':catalog,'schema':'^system$','procedure':'^register_table$','privileges':['EXECUTE']} for user,catalog in sorted(publisher_catalogs)]
    # Explicit default denial also overrides Trino's special default system-catalog access.
    rules['catalogs'].append({'catalog':'.*','allow':'none'})
    rules['queries']=[{'user':user,'allow':['execute']} for user in sorted(users)]
    # Trino 484 feeds new_user through Java Matcher.appendReplacement before
    # compiling it as a regex. Preserve regex backslashes and anchors across
    # that extra interpretation; otherwise '.' broadens and '$' throws.
    rules['impersonation']=[{'original_user':broker,'new_user':user.replace('\\','\\\\').replace('$','\\$'),'allow':True} for user in sorted(reader_users)]
    return rules


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    args=parser.parse_args()
    print(json.dumps(compile_rules(json.loads(Path(args.config).read_text())),indent=2))
