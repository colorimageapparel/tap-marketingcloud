#!/usr/bin/env python3

import json
import sys
import singer

from singer import utils
from singer import metadata
from tap_marketingcloud.state import save_state
from tap_marketingcloud.client import get_auth_stub
from tap_marketingcloud.endpoints.campaigns import CampaignDataAccessObject
from tap_marketingcloud.endpoints.content_areas import ContentAreaDataAccessObject
from tap_marketingcloud.endpoints.emails import EmailDataAccessObject
from tap_marketingcloud.endpoints.events import EventDataAccessObject
from tap_marketingcloud.endpoints.folders import FolderDataAccessObject
from tap_marketingcloud.endpoints.lists import ListDataAccessObject
from tap_marketingcloud.endpoints.list_sends import ListSendDataAccessObject
from tap_marketingcloud.endpoints.list_subscribers import ListSubscriberDataAccessObject
from tap_marketingcloud.endpoints.sends import SendDataAccessObject
from tap_marketingcloud.endpoints.subscribers import SubscriberDataAccessObject
from tap_marketingcloud.endpoints.triggered_send import TriggeredSendDataAccessObject
from tap_marketingcloud.endpoints.campaign_assets import CampaignAssetDataAccessObject
from tap_marketingcloud.endpoints.interactions import InteractionsDataAccessObject
from tap_marketingcloud.endpoints.link_sends import LinkSendDataAccessObject
from tap_marketingcloud.endpoints.assets import AssetDataAccessObject


LOGGER = singer.get_logger()  # noqa

REQUIRED_CONFIG_KEYS = [
    'client_id',
    'client_secret',
    'start_date'
]


AVAILABLE_STREAM_ACCESSORS = [
    CampaignDataAccessObject,
    ContentAreaDataAccessObject,
    EmailDataAccessObject,
    EventDataAccessObject,
    FolderDataAccessObject,
    ListDataAccessObject,
    ListSendDataAccessObject,
    ListSubscriberDataAccessObject,
    SendDataAccessObject,
    SubscriberDataAccessObject,
    TriggeredSendDataAccessObject,
    CampaignAssetDataAccessObject,
    InteractionsDataAccessObject,
    LinkSendDataAccessObject,
    AssetDataAccessObject
]

# run discover mode
def do_discover(args):
    LOGGER.info("Starting discovery.")

    config = args.config
    state = args.state

    auth_stub = get_auth_stub(config)

    catalog = []

    for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
        stream_accessor = available_stream_accessor(
            config, state, auth_stub, None)

        catalog += stream_accessor.generate_catalog()

    print(json.dumps({'streams': catalog}, indent=4))

# check if the stream is selected or not
def _is_selected(catalog_entry):
    mdata = metadata.to_map(catalog_entry['metadata'])
    return singer.should_sync_field(metadata.get(mdata, (), 'inclusion'),
                                    metadata.get(mdata, (), 'selected'),
                                    default=False)

# run sync mode
def do_sync(args):
    LOGGER.info("Starting sync.")

    config = args.config
    state = args.state
    catalog = args.properties

    success = True

    auth_stub = get_auth_stub(config)

    stream_accessors = []

    subscriber_selected = False
    subscriber_catalog = None
    send_link_catalog = None
    send_selected = False
    send_link_selected = False
    list_subscriber_selected = False

    for stream_catalog in catalog.get('streams'):
        stream_accessor = None

        if not _is_selected(stream_catalog):
            LOGGER.info("'{}' is not marked selected, skipping."
                        .format(stream_catalog.get('stream')))
            continue

        # for 'subscriber' stream if it is selected, add values for 'subscriber_catalog' and
        # 'subscriber_selected', and it will replicated via 'list_subscribers' stream
        # The 'subscribers' stream is the child stream of 'list_subscribers'
        # When we sync 'list_subscribers', it makes the list of subscriber's
        # 'SubscriberKey' that were returned as part of 'list_subscribers' records
        # and pass that list to 'subscribers' stream and thus 'subscribers' stream
        # will only sync records of subscribers that are present in the list.
        # Hence, for different start dates the 'SubscriberKey' list will differ and
        # thus 'subscribers' records will also be different for different start dates.
        if SubscriberDataAccessObject.matches_catalog(stream_catalog):
            subscriber_selected = True
            subscriber_catalog = stream_catalog
            LOGGER.info("'subscriber' selected, will replicate via "
                        "'list_subscriber'")
            continue

        if LinkSendDataAccessObject.matches_catalog(stream_catalog):
            send_link_selected = True
            send_link_catalog = stream_catalog
            LOGGER.info("'send_link' selected, will replicate via "
                        "'send'")
            continue

        if ListSubscriberDataAccessObject.matches_catalog(stream_catalog):
            list_subscriber_selected = True

        if SendDataAccessObject.matches_catalog(stream_catalog):
            send_selected = True

        for available_stream_accessor in AVAILABLE_STREAM_ACCESSORS:
            if available_stream_accessor.matches_catalog(stream_catalog):
                stream_accessors.append(available_stream_accessor(
                    config, state, auth_stub, stream_catalog))

                break

    # do not replicate 'subscriber' stream without selecting 'list_subscriber' stream
    if subscriber_selected and not send_link_selected:
        LOGGER.fatal('Cannot replicate `send_link` without '
                     '`send`. Please select `send` '
                     'and try again.')
        sys.exit(1)

    if send_selected and not list_subscriber_selected:
        LOGGER.fatal('Cannot replicate `subscriber` without '
                     '`list_subscriber`. Please select `list_subscriber` '
                     'and try again.')
        sys.exit(1)

    for stream_accessor in stream_accessors:
        if isinstance(stream_accessor, ListSubscriberDataAccessObject) and \
           subscriber_selected:
            stream_accessor.replicate_subscriber = True
            stream_accessor.subscriber_catalog = subscriber_catalog

        if isinstance(stream_accessor, SendDataAccessObject) and \
           send_link_selected:
            stream_accessor.send_link_catalog = send_link_catalog

        try:
            stream_accessor.state = state
            stream_accessor.sync()
            state = stream_accessor.state

        except Exception as e:
            LOGGER.exception(e)
            LOGGER.error('Failed to sync endpoint, moving on!')
            success = False

    save_state(state)

    return success


@utils.handle_top_exception(LOGGER)
def main():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    success = True

    if args.discover:
        do_discover(args)
    elif args.properties:
        success = do_sync(args)
    else:
        LOGGER.info("No properties were selected")

    if success:
        LOGGER.info("Completed successfully, exiting.")
        sys.exit(0)
    else:
        LOGGER.info("Run failed, exiting.")
        sys.exit(1)

if __name__ == '__main__':
    main()
