import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table


LOGGER = singer.get_logger()


class AssetDataAccessObject(DataAccessObject):

    TABLE = 'asset'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['modifiedDate']

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE

        # pass config to return start date if not bookmark is found
        retrieve_all_since = get_last_record_value_for_table(self.state, table, self.config)

        if retrieve_all_since is not None:
            filter = f'modifiedDate gte {retrieve_all_since}'

        cursor = request(
            'Asset',
            FuelSDK.ET_Asset,
            self.auth_stub,
            props={"$pageSize": 50, "$page": 1, "page": 1, "$filter": filter, "$orderBy": "modifiedDate asc"})

        catalog_copy = copy.deepcopy(self.catalog)

        for asset in cursor:
            asset = self.filter_keys_and_parse(asset)
            self.state = incorporate(self.state,
                                     table,
                                     'modifiedDate',
                                     asset.get('modifiedDate'))

            self.write_records_with_transform(asset, catalog_copy, table)

        save_state(self.state)
