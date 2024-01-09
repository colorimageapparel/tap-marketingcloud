import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.pagination import get_date_page, before_date, increment_date
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table, get_end_date

LOGGER = singer.get_logger()


class LinkSendDataAccessObject(DataAccessObject):

    TABLE = 'link_send'
    KEY_PROPERTIES = ['LinkID', 'SendID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['LinkID'] = to_return.get('Link', {}).get('ID')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_LinkSend

        search_filter = None

        # pass config to return start date if not bookmark is found
        start = get_last_record_value_for_table(self.state, table, self.config)
        end_date = get_end_date(self.config)

        pagination_unit = 'minutes'
        pagination_quantity = 15

        unit = {pagination_unit: int(pagination_quantity)}

        end = increment_date(start, unit)

        LOGGER.info('Before date: %s', before_date(start, end_date))

        while before_date(start, end_date):
            search_filter = get_date_page('ModifiedDate', start, unit)

            catalog_copy = copy.deepcopy(self.catalog)

            stream = request('LinkSendDataAccessObject',
                            selector,
                            self.auth_stub,
                            search_filter,
                            batch_size=self.batch_size)
            for content_area in stream:
                content_area = self.filter_keys_and_parse(content_area)

                self.state = incorporate(self.state,
                                        table,
                                        'ModifiedDate',
                                        content_area.get('ModifiedDate'))

                self.write_records_with_transform(content_area, catalog_copy, table)

            save_state(self.state)
            start = end
            end = increment_date(start, unit)
