import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.endpoints.link_sends import LinkSendDataAccessObject
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table
from tap_marketingcloud.util import partition_all

LOGGER = singer.get_logger()

def _get_send_id(send):
    # return the 'SubscriberKey' of the subscriber
    return send.ID

class SendDataAccessObject(DataAccessObject):

    TABLE = 'send'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def __init__(self, config, state, auth_stub, catalog):
        super().__init__(
            config, state, auth_stub, catalog)

        self.send_link_catalog = None

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Send

        search_filter = None

        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        linksend_dao = LinkSendDataAccessObject(
            self.config,
            self.state,
            self.auth_stub,
            self.send_link_catalog)

        stream = request('Send',
                         selector,
                         self.auth_stub,
                         search_filter,
                         batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)
        batch_size = 50
        linksend_dao.write_schema()

        for send_batch in partition_all(stream, batch_size):
            for send in send_batch:
                send = self.filter_keys_and_parse(
                    send)

                self.state = incorporate(self.state,
                                    table,
                                    'ModifiedDate',
                                    send.get('ModifiedDate'))

                self.write_records_with_transform(send, catalog_copy, table)

            send_ids = list(map(
                _get_send_id, send_batch))

            linksend_dao.pull_link_send_batch(send_ids)

            # Send state message to target
            save_state(self.state)
