"""DataSlate's original Apache-2.0 Airflow provider."""

def get_provider_info():
    return {
        'package-name':'dataslate-airflow-provider','name':'DataSlate',
        'description':'Governed DataSlate operations with durable request recovery.',
        'versions':['0.1.0a1'],
        'integrations':[{'integration-name':'DataSlate','external-doc-url':'https://github.com/NimeshBuilds/dataslate','tags':['service']}],
        'hooks':[{'integration-name':'DataSlate','python-modules':['dataslate_airflow.hooks']}],
        'operators':[{'integration-name':'DataSlate','python-modules':['dataslate_airflow.operators']}],
        'sensors':[{'integration-name':'DataSlate','python-modules':['dataslate_airflow.operators']}],
        'connection-types':[{'connection-type':'dataslate','hook-class-name':'dataslate_airflow.hooks.DataSlateHook'}],
    }
