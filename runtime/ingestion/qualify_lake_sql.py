"""Probe only DataSlate's synthetic local Spark/HMS/Trino fixture over TLS.

This is an explicit test helper, not the production publication adapter. It
registers fixed tables in the disposable default schema; never point it at a
customer engine. Every SQL submission is attempted once, without fallback.
"""
import argparse
import json
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


class Probe:
    def __init__(self, endpoint, ca, tokens):
        self.endpoint = endpoint.rstrip('/')
        origin = urllib.parse.urlsplit(self.endpoint)
        if origin.scheme != 'https' or origin.hostname != '127.0.0.1' or origin.path or origin.query or origin.fragment or origin.username:
            raise ValueError('An explicit loopback HTTPS fixture endpoint is required')
        self.origin = origin.netloc
        self.tokens = tokens
        self.client = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=str(ca))))

    def query(self, sql, principal='dataslate-publisher', user=None):
        headers = {'Authorization': 'Bearer '+self.tokens[principal], 'X-Trino-User': user or principal}
        request = urllib.request.Request(self.endpoint+'/v1/statement', data=sql.encode(), headers=headers)
        rows, columns, query_id = [], [], None
        deadline = time.monotonic()+60
        for _ in range(1000):
            if time.monotonic() >= deadline:
                raise RuntimeError('Fixture query deadline exceeded; outcome unknown, no retry')
            try:
                with self.client.open(request, timeout=10) as response:
                    body = response.read(1024*1024+1)
            except urllib.error.HTTPError as error:
                if error.code in (401, 403):
                    return {'denied': True, 'httpStatus': error.code}
                raise RuntimeError('Unexpected fixture HTTP response') from None
            if len(body) > 1024*1024:
                raise RuntimeError('Fixture response exceeded bound')
            document = json.loads(body)
            query_id = document.get('id', query_id)
            if document.get('error'):
                error = document['error']
                return {'error': error['errorName'], 'denied': error['errorName'] == 'PERMISSION_DENIED', 'queryId': query_id}
            rows.extend(document.get('data', [])); columns = document.get('columns', columns)
            if len(rows) > 1000:
                raise RuntimeError('Fixture row bound exceeded')
            if 'nextUri' not in document:
                return {'rows': rows, 'columns': columns, 'queryId': query_id}
            next_uri = document['nextUri']; parsed = urllib.parse.urlsplit(next_uri)
            if parsed.scheme != 'https' or parsed.netloc != self.origin or not parsed.path.startswith('/v1/statement/') or parsed.fragment or parsed.username:
                raise RuntimeError('Fixture returned an unexpected continuation origin')
            request = urllib.request.Request(next_uri, headers=headers)
            time.sleep(.03)
        raise RuntimeError('Fixture page bound exceeded')


def require(value, message):
    if not value:
        raise RuntimeError(message)


def qualify(probe):
    evidence = {'scope': 'synthetic local shared volume; Spark Delta4.3.1, HMS4.2.1, Trino484; no cloud or production OIDC qualification', 'checks': {}}
    for table in ['orders', 'timestamp_precision']:
        result = probe.query("CALL lake.system.register_table(schema_name => 'default', table_name => '"+table+"', table_location => 'file:///lake/"+table+"/')")
        require('rows' in result, 'Explicit fixture registration did not succeed: '+str(result))
        evidence['checks']['register_'+table] = result['queryId']
    properties = probe.query('SELECT key, value FROM lake.default."orders$properties"')
    require('rows' in properties, 'Could not read structured fixture table properties')
    keys = [row[0] for row in properties['rows']]
    require(len(set(keys)) == len(keys), 'Duplicate property keys are ambiguous')
    mapped = dict(properties['rows'])
    require(mapped.get('location') == 'file:///lake/orders/', 'Registered fixture location differs')
    require(mapped.get('delta.minReaderVersion') == '1' and mapped.get('delta.minWriterVersion') == '2', 'Unexpected fixture Delta protocol')
    evidence['checks']['structuredLocationAndProtocol'] = True
    history = probe.query('SELECT version FROM lake.default."orders$history" ORDER BY version')
    require(history.get('rows') == [[0]], 'Expected first Delta version is absent')
    evidence['checks']['expectedVersion'] = '0'
    rows = probe.query('SELECT id, region, email, amount, active, ordered_on FROM lake.default.orders FOR VERSION AS OF 0 ORDER BY id', 'dataslate-broker', 'team-alice')
    require(rows.get('rows') == [[1, 'US', 'REDACTED', 10.5, True, '2026-11-07'], [3, 'US', 'REDACTED', 30.25, True, '2026-11-08']], 'Governed reader rows, mask or primitive values differ')
    evidence['checks']['governedRowsAndMask'] = rows['rows']
    for name, sql, principal, user in [
        ('reader_metadata', 'SELECT * FROM lake.default."orders$properties"', 'dataslate-broker', 'team-alice'),
        ('reader_insert', "INSERT INTO lake.default.orders VALUES (9,'US','x',1,true,DATE '2026-11-07')", 'dataslate-broker', 'team-alice'),
        ('reader_delete', 'DELETE FROM lake.default.orders WHERE id=1', 'dataslate-broker', 'team-alice'),
        ('reader_drop', 'DROP TABLE lake.default.orders', 'dataslate-broker', 'team-alice'),
        ('reader_register', "CALL lake.system.register_table('default','orders','file:///lake/orders/')", 'dataslate-broker', 'team-alice'),
        ('ungranted_reader', 'SELECT * FROM lake.default.orders', 'team-bob', None),
        ('broker_to_publisher', 'SELECT * FROM lake.default."orders$properties"', 'dataslate-broker', 'dataslate-publisher'),
        ('publisher_other_target', "CALL lake.system.register_table('default','ungranted','file:///lake/orders/')", 'dataslate-publisher', None),
        ('publisher_other_schema', "CALL lake.system.register_table('other','orders','file:///lake/orders/')", 'dataslate-publisher', None),
        ('publisher_other_procedure', "CALL lake.system.unregister_table('default','orders')", 'dataslate-publisher', None),
    ]:
        result = probe.query(sql, principal, user)
        require(result.get('denied') is True, 'Expected authorization denial: '+name+' '+str(result))
        evidence['checks'][name] = 'denied'
    precision = probe.query('SELECT event_at FROM lake.default.timestamp_precision')
    require('rows' in precision and len(precision['rows']) == 1, 'Timestamp boundary could not be observed')
    evidence['timestampBoundary'] = {'sparkInput': '2026-11-07T12:00:00.123456Z', 'trinoValue': precision['rows'][0][0], 'trinoType': precision['columns'][0]['type'], 'publicationSupported': False}
    require('123456' not in str(precision['rows'][0][0]), 'Timestamp behavior changed; revisit conservative publication exclusion')
    return evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', required=True); parser.add_argument('--private-fixture', type=Path, required=True); parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(not args.output.exists(), 'Refusing to overwrite previous qualification evidence')
    tokens = {name: (args.private_fixture/(name+'.token')).read_text().strip() for name in ['dataslate-publisher', 'dataslate-broker', 'team-bob']}
    evidence = qualify(Probe(args.endpoint, args.private_fixture/'ca.crt', tokens))
    with args.output.open('x') as handle:
        json.dump(evidence, handle, indent=2); handle.write('\n')
    print('Local cross-engine registration, governed rows and authorization probes passed; timestamp precision limitation recorded.')


if __name__ == '__main__':
    main()
