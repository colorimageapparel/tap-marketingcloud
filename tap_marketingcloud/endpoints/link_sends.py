import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)

LOGGER = singer.get_logger()


class LinkSendDataAccessObject(DataAccessObject):

    TABLE = 'link_send'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['LinkID'] = to_return.get('Link', {}).get('ID')
        to_return['URL'] = to_return.get('Link', {}).get('URL')
        to_return['TotalClicks'] = to_return.get('Link', {}).get('TotalClicks')
        to_return['UniqueClicks'] = to_return.get('Link', {}).get('UniqueClicks')
        to_return['LastClicked'] = to_return.get('Link', {}).get('LastClicked')
        to_return['Alias'] = to_return.get('Link', {}).get('Alias')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def pull_link_send_batch(self, send_ids):
        if not send_ids:
            return

        table = self.__class__.TABLE
        _filter = {}

        if len(send_ids) == 1:
            _filter = {
                'Property': 'SendID',
                'SimpleOperator': 'equals',
                'Value': send_ids[0]
            }

        elif len(send_ids) > 1:
            _filter = {
                'Property': 'SendID',
                'SimpleOperator': 'IN',
                'Value': send_ids
            }
        else:
            LOGGER.info('Got empty set of subscriber keys, moving on')
            return

        catalog_copy = copy.deepcopy(self.catalog)
        stream = request(
            'LinkSendDataAccessObject', FuelSDK.ET_LinkSend, self.auth_stub, _filter, batch_size=self.batch_size)

        for link_send in stream:
            link_send = self.filter_keys_and_parse(link_send)
            self.write_records_with_transform(link_send, catalog_copy, table)

    @exacttarget_error_handling
    def sync_data(self):
        pass
