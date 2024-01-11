import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)

LOGGER = singer.get_logger()

class InteractionsDataAccessObject(DataAccessObject):

    TABLE = 'interaction'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'FULL_TABLE'

    @exacttarget_error_handling
    def sync_data(self):
        cursor = request('Interactions',
                         FuelSDK.ET_Interactions,
                         self.auth_stub,
                         props={"$pageSize": self.batch_size, "$page": 1, "page": 1, "extras": "activities"})

        catalog_copy = copy.deepcopy(self.catalog)

        for interaction in cursor:
            interaction = self.filter_keys_and_parse(interaction)

            self.write_records_with_transform(interaction, catalog_copy, self.TABLE)
