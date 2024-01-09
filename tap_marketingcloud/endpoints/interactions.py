import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class InteractionsDataAccessObject(DataAccessObject):

    TABLE = 'interaction'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['modifiedDate']

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Interactions

        search_filter = None

        # pass config to return start date if not bookmark is found
        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            search_filter = {
                'Property': 'modifiedDate',
                'SimpleOperator': 'greaterThan',
                'Value': retrieve_all_since
            }

        stream = request('Interactions',
                         selector,
                         self.auth_stub,
                         search_filter,
                         props={"extras": "activities"},
                         batch_size=self.batch_size)

        catalog_copy = copy.deepcopy(self.catalog)

        for interaction in stream:
            interaction = self.filter_keys_and_parse(interaction)
            self.state = incorporate(self.state,
                                     table,
                                     'modifiedDate',
                                     interaction.get('modifiedDate'))

            self.write_records_with_transform(interaction, catalog_copy, table)

        save_state(self.state)
