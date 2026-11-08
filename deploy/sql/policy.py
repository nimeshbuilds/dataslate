#!/usr/bin/env python3
"""Compile administrator-owned exact grants to Trino 484 file access rules.

The output is engine configuration, not a proof that the running engine loaded
it. Filters and masks are trusted administrator SQL; caller queries cannot set
them. No schema ownership, writes, external functions or cross-user query access
is granted by this profile.
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
    if not isinstance(config, dict) or set(config) != {'brokerUser', 'grants'}:
        raise ValueError('Expected brokerUser and grants only')
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
    rules['catalogs']=[{'user':user,'catalog':catalog,'allow':'read-only'} for user,catalog in sorted(catalogs)]
    # Explicit default denial also overrides Trino's special default system-catalog access.
    rules['catalogs'].append({'catalog':'.*','allow':'none'})
    rules['queries']=[{'user':user,'allow':['execute']} for user in sorted(users)]
    # Trino 484 feeds new_user through Java Matcher.appendReplacement before
    # compiling it as a regex. Preserve regex backslashes and anchors across
    # that extra interpretation; otherwise '.' broadens and '$' throws.
    rules['impersonation']=[{'original_user':broker,'new_user':user.replace('\\','\\\\').replace('$','\\$'),'allow':True} for user in sorted(users)]
    return rules


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    args=parser.parse_args()
    print(json.dumps(compile_rules(json.loads(Path(args.config).read_text())),indent=2))
