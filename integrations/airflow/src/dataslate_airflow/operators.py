"""Explicit submit + rescheduling observation; no busy worker while compute runs."""
from airflow.sdk import BaseOperator, BaseSensorOperator, PokeReturnValue
from airflow.exceptions import AirflowException
from airflow.sdk.exceptions import AirflowFailException
from dataslate import DataSlateError
from .hooks import DataSlateHook, CredentialError
from .core import OutcomeError, recovery_key, request_fields, reference_fields, submit, poll


def _safe_error(error):
    if isinstance(error,DataSlateError):
        # The API's raw error body is deliberately excluded from Airflow logs.
        return AirflowException('DataSlate request was not completed; retain the same task/run identity for recovery')
    return AirflowFailException('Invalid DataSlate task or connection configuration')


class DataSlateSubmitOperator(BaseOperator):
    template_fields=('dataslate_conn_id','request')
    template_fields_renderers={'request':'json'}
    ui_color='#DDE9DE'
    custom_operator_name='DataSlate submit'

    def __init__(self, *, operation, request, dataslate_conn_id='dataslate_default', **kwargs):
        super().__init__(**kwargs)
        self.operation=operation;self.request=request;self.dataslate_conn_id=dataslate_conn_id

    def execute(self,context):
        try:
            request_fields(self.operation,self.request)
            key=recovery_key(context,self.operation)
            client=DataSlateHook(self.dataslate_conn_id).get_conn()
            return submit(client,self.operation,self.request,key)
        except (DataSlateError,CredentialError,ValueError,OSError) as exc:raise _safe_error(exc) from None


class DataSlateOperationSensor(BaseSensorOperator):
    template_fields=(*BaseSensorOperator.template_fields,'dataslate_conn_id','reference')
    template_fields_renderers={'reference':'json'}
    ui_color='#DDE9DE'
    custom_operator_name='DataSlate await outcome'

    def __init__(self, *, reference, dataslate_conn_id='dataslate_default', **kwargs):
        kwargs.setdefault('mode','reschedule');kwargs.setdefault('poke_interval',30);kwargs.setdefault('timeout',86400)
        # Airflow output['resourceId'] resolves a separate XCom key, not a
        # dictionary field inside return_value. Expose only our minimal reference.
        kwargs.setdefault('multiple_outputs',True)
        if any(kwargs.get(key,False) for key in ('soft_fail','silent_fail','never_fail')):
            raise ValueError('DataSlate sensors must fail on failed or unverified outcomes')
        super().__init__(**kwargs)
        self.reference=reference;self.dataslate_conn_id=dataslate_conn_id

    def poke(self,context):
        try:
            reference_fields(self.reference)
            client=DataSlateHook(self.dataslate_conn_id).get_conn()
            return PokeReturnValue(poll(client,self.reference),self.reference)
        except OutcomeError as exc:raise AirflowFailException(str(exc)) from None
        except (DataSlateError,CredentialError,ValueError,OSError) as exc:raise _safe_error(exc) from None

    # Killing/clearing a sensor stops observation, never infers remote cancellation.
