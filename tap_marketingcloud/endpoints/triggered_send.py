import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class TriggeredSendDataAccessObject(DataAccessObject):

    TABLE = 'triggered_send'
    KEY_PROPERTIES = ['ObjectID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_TriggeredSend

        search_filter = None

        # pass config to return start date if not bookmark is found
        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'ModifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        catalog_copy = copy.deepcopy(self.catalog)

        stream = request('TriggeredSendDataAccessObject',
                         selector,
                         self.auth_stub,
                         search_filter,
                         props=list(catalog_copy.get('schema').get('properties').keys()),
                         batch_size=self.batch_size)
        for content_area in stream:
            content_area = self.filter_keys_and_parse(content_area)

            self.state = incorporate(self.state,
                                     table,
                                     'ModifiedDate',
                                     content_area.get('ModifiedDate'))

            self.write_records_with_transform(content_area, catalog_copy, table)

        save_state(self.state)
