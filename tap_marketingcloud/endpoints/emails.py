import FuelSDK
import copy
import singer

from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.pagination import get_date_page, before_date, increment_date
from tap_marketingcloud.state import incorporate, save_state, \
    get_last_record_value_for_table, get_end_date

LOGGER = singer.get_logger()


class EmailDataAccessObject(DataAccessObject):

    TABLE = 'email'
    KEY_PROPERTIES = ['ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['ModifiedDate']

    def parse_object(self, obj):
        to_return = obj.copy()
        content_area_ids = []

        for content_area in to_return.get('ContentAreas', []):
            content_area_ids.append(content_area.get('ID'))

        to_return['EmailID'] = to_return.get('Email', {}).get('ID')
        to_return['ContentAreaIDs'] = content_area_ids

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        selector = FuelSDK.ET_Email

        search_filter = None
        # pass config to return start date if not bookmark is found
        start = get_last_record_value_for_table(self.state, table, self.config)
        end_date = get_end_date(self.config)

        if start is None:
            raise RuntimeError('start_date not defined!')

        pagination_unit = self.config.get(
            'pagination__{}_interval_unit'.format(table), 'minutes')
        pagination_quantity = self.config.get(
            'pagination__{}_interval_quantity'.format(table), 60)

        unit = {pagination_unit: int(pagination_quantity)}

        end = increment_date(start, unit)

        LOGGER.info('Before date: %s', before_date(start, end_date))

        while before_date(start, end_date):
            LOGGER.info('Start: %s, end: %s, date comparison: %s', start, end_date, before_date(start, end_date))
            LOGGER.info("Fetching {} from {} to {}"
                        .format(table, start, end))

            search_filter = get_date_page('ModifiedDate', start, unit)

            stream = request('Email',
                                selector,
                                self.auth_stub,
                                search_filter,
                                batch_size=self.batch_size)

            catalog_copy = copy.deepcopy(self.catalog)

            for event in stream:
                event = self.filter_keys_and_parse(event)

                self.state = incorporate(self.state, table, 'ModifiedDate', event.get('ModifiedDate'))

                self.write_records_with_transform(event, catalog_copy, table)

            self.state = incorporate(self.state, table, 'ModifiedDate', start)

            save_state(self.state)

            start = end
            end = increment_date(start, unit)
