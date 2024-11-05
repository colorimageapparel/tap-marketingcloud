import FuelSDK
import copy
import singer

from datetime import datetime
from tap_marketingcloud.client import request
from tap_marketingcloud.dao import (DataAccessObject, exacttarget_error_handling)
from tap_marketingcloud.pagination import get_date_page, before_date, increment_date
from tap_marketingcloud.state import incorporate, save_state, get_last_record_value_for_table, get_end_date


LOGGER = singer.get_logger()


class EventDataAccessObject(DataAccessObject):

    TABLE = 'event'
    KEY_PROPERTIES = ['SendID', 'EventType', 'SubscriberKey', 'EventDate', 'ID']
    REPLICATION_METHOD = 'INCREMENTAL'
    REPLICATION_KEYS = ['EventDate']

    def parse_object(self, obj):
        to_return = obj.copy()

        to_return['ID'] = to_return.get('ID', 'N/A')

        return super().parse_object(to_return)

    @exacttarget_error_handling
    def sync_data(self):
        table = self.__class__.TABLE
        endpoints = {
            'sent': FuelSDK.ET_SentEvent,
            'click': FuelSDK.ET_ClickEvent,
            'bounce': FuelSDK.ET_BounceEvent,
            'unsub': FuelSDK.ET_UnsubEvent
        }

        for event_name, selector in endpoints.items():
            search_filter = None

            # pass config to return start date if not bookmark is found
            start = get_last_record_value_for_table(self.state, event_name, self.config)
            end_date = get_end_date(self.config)

            if start is None:
                raise RuntimeError('start_date not defined!')

            pagination_unit = self.config.get(
                'pagination__{}_interval_unit'.format(event_name), 'minutes')
            pagination_quantity = self.config.get(
                'pagination__{}_interval_quantity'.format(event_name), 10)

            unit = {pagination_unit: int(pagination_quantity)}

            end = increment_date(start, unit)

            LOGGER.info('Before date: %s', before_date(start, end_date))

            while before_date(start, end_date):
                LOGGER.info('Start: %s, end: %s, date comparison: %s', start, end_date, before_date(start, end_date))
                LOGGER.info("Fetching {} from {} to {}"
                            .format(event_name, start, end))

                search_filter = get_date_page('EventDate', start, unit)

                stream = request(event_name,
                                 selector,
                                 self.auth_stub,
                                 search_filter,
                                 batch_size=self.batch_size)

                catalog_copy = copy.deepcopy(self.catalog)

                for event in stream:
                    event = self.filter_keys_and_parse(event)

                    self.state = incorporate(self.state, event_name, 'EventDate', event.get('EventDate'))

                    if event.get('SubscriberKey') is None:
                        LOGGER.info("SubscriberKey is NULL so ignoring {} record with SendID: {} and EventDate: {}"
                                    .format(event_name,
                                            event.get('SendID'),
                                            event.get('EventDate')))
                        continue

                    self.write_records_with_transform(event, catalog_copy, table)

                self.state = incorporate(self.state, event_name, 'EventDate', start, delay_in_day=1)

                save_state(self.state)

                start = end
                end = increment_date(start, unit)
