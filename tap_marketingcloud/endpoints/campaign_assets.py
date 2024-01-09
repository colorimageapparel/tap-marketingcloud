import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)

LOGGER = singer.get_logger()


class CampaignAssetDataAccessObject(DataAccessObject):

    TABLE = 'campaign_asset'
    KEY_PROPERTIES = ['id']
    REPLICATION_METHOD = 'FULL_TABLE'

    @exacttarget_error_handling
    def sync_data(self):
        cursor = request(
            'Campaign',
            FuelSDK.ET_Campaign,
            self.auth_stub,
            # use $pageSize and $page in the props for
            # this stream as it calls using REST API
            props={"$pageSize": self.batch_size, "$page": 1, "page": 1})

        catalog_copy = copy.deepcopy(self.catalog)

        for campaign in cursor:
            cursor = request(
                'Campaign Assets',
                FuelSDK.ET_Campaign_Asset,
                self.auth_stub,
                # use $pageSize and $page in the props for
                # this stream as it calls using REST API
                props={"$pageSize": self.batch_size, "$page": 1, "page": 1, "id": campaign.get("id")})

            catalog_copy = copy.deepcopy(self.catalog)

            for campaign_asset in cursor:
                campaign_asset = self.filter_keys_and_parse(campaign_asset)
                self.write_records_with_transform(campaign_asset, catalog_copy, self.TABLE)
