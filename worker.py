#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  @copyright 2018, Jeff Tanner, jeff00seattle

import sys
import os
import time
import gzip
import copy
import urllib
import gc
import re
import ujson as json
import datetime as dt
from concurrent import futures
from collections import defaultdict

from safe_cast import (
    safe_float,
    safe_int,
    safe_str
)
# from pprintpp import pprint
# import ujson as json

from urllib.parse import unquote as urldecode

from logging_fortified import (get_logger)

from requests_worker import (
    __python_required_version__
)
from requests_worker.requests_worker_factory import RequestsWorkerCallerFactory
from requests_worker.requests_worker_base import RequestsWorkerBase

from requests_worker.errors import (
    get_exception_message,
    error_name,
    error_desc,
    print_traceback,
    RequestsWorkerStatusCode
)
from requests_worker.support import (
    SECONDS_FOR_23_AND_HALF_HOURS,
    python_check_version,
    base_class_name,
    create_download_dir,
    validate_config_job_dates,
    sorted_nicely,
    create_hash_key,
)
from requests_fortified.support import (
    validate_json_response,
    HEADER_CONTENT_TYPE_APP_JSON,)

from requests_worker.exceptions import (
    RequestsWorkerBaseError,
    RequestsWorkerClientUnauthorizedError,
    RequestsWorkerClientBadRequestError,
    RequestsWorkerAuthenticationError,
    RequestsWorkerConfigError,
    RequestsWorkerSoftwareError,
    RequestsWorkerClientError,
    RequestsWorkerValueError,
    RequestsWorkerError,)
from requests_fortified.exceptions import (
    RequestsFortifiedBaseError,
    RequestsFortifiedAuthenticationError,
    RequestsFortifiedClientError,)

__FACEBOOK_GRAPH_API_VERSION = 'v2.12'
FACEBOOK_GRAPH_API_URL = f'https://graph.facebook.com/{__FACEBOOK_GRAPH_API_VERSION}'

DOWNLOAD_DIR = os.path.dirname(os.path.realpath(__file__)) + '/tmp'
UPLOAD_FILE_NAME = "facebook_ads"

python_check_version(__python_required_version__)

_FACEBOOK_INSIGHTS_FIELDS = [
    "date_start",
    "date_stop",

    "account_id",
    "account_name",
    "account_currency",

    "campaign_id",
    "campaign_name",

    "ad_id",
    "ad_name",

    "adset_id",
    "adset_name",

    "objective",

    "actions",
    "action_values",
    "unique_actions",

    "spend",
    "clicks",
    "unique_clicks",
    "unique_ctr",

    "impressions",
    "reach",  # Replaced unique_impressions

    "video_p100_watched_actions"
]

_FACEBOOK_INSIGHTS_PARAMS = {
    "access_token": None,
    "time_range": {},
    "async": "true",
    "action_report_time": "impression",  # Determines the report time of action stats.
    # For example, if a person saw the ad on Jan 1st but converted on Jan 2nd,
    # when you query the API with action_report_time=impression, you will see a conversion on Jan 1st.
    # When you query the API with action_report_time=conversion, you will see a conversion on Jan 2nd.
    "default_summary": "false",
    # "breakdowns": ["publisher_platform", "impression_device", "device_platform"],
    "breakdowns": ["impression_device"],
    "action_breakdowns": ["action_type","action_target_id"],
    "level": "ad"}


_FACEBOOK_INSIGHTS_ACTIONS_MAPPING = {
    'app_custom_event.fb_mobile_achievement_unlocked': 'received_events_achievement_unlocked',
    'app_custom_event.fb_mobile_add_payment_info': 'received_events_added_payment_info',
    'app_custom_event.fb_mobile_add_to_cart': 'received_events_add_to_cart',
    'app_custom_event.fb_mobile_add_to_wishlist': 'received_events_add_to_wishlist',
    'app_custom_event.fb_mobile_complete_registration': 'received_events_registration',
    'app_custom_event.fb_mobile_content_view': 'received_events_content_view',
    'app_custom_event.fb_mobile_initiated_checkout': 'received_events_checkout_initiated',
    'app_custom_event.fb_mobile_level_achieved': 'received_events_level_achieved',
    'app_custom_event.fb_mobile_purchase': 'received_events_purchase',
    'app_custom_event.fb_mobile_rate': 'received_events_rated',
    'app_custom_event.fb_mobile_search': 'received_events_search',
    'app_custom_event.fb_mobile_spent_credits': 'received_events_spent_credits',
    'app_custom_event.fb_mobile_tutorial_completion': 'received_events_tutorial_complete',
    'app_custom_event.other': 'received_events_other',
    'app_custom_event': 'received_events_other'}

_GRANULARITY_HOUR = 'hour'
_GRANULARITY_DAY = 'day'

class FacebookAdsWorker(RequestsWorkerBase):
    _WORKER_NAME = "Facebook Ads Worker"
    _WORKER_VERSION = "0.0.2"

    _CACHE_NAME = 'facebook_ads'
    UPLOAD_FILE_NAME = 'facebook_ads'

    _GRANULARITY = _GRANULARITY_HOUR

    _MAX_WORKERS = 5
    agency_ids_map = {}

    fb_data_tracking = {
        _GRANULARITY_DAY: {
            "hash_keys": {},
            "enabled": True
        },
        _GRANULARITY_HOUR: {
            "hash_keys": {},
            "enabled": True
        }
    }

    #
    # Initialize
    #
    def __init__(
            self,
            config_path=None):
        """Initialize
        """
        super(FacebookAdsWorker, self).__init__(
            payload=config_path,
            cache_name=self._CACHE_NAME,
        )

        self.logger = get_logger(
            logger_name=self._WORKER_NAME,
            logger_version=self._WORKER_VERSION,
            logger_format=self.logger_format,
            logger_level=self.logger_level,
            logger_output=self.logger_output
        )
    #
    # Worker:
    #
    def work(
        self,
        config_job=None,
        config_request_credentials=None
    ):
        upload_response = (None, 0, 0)

        self.logger.info('Validate Client Credentials')
        self.validate_client_credentials(required=['access_token'], credentials=config_request_credentials)

        self.logger.info('Begin')
        (_, _, datetime_start, datetime_end,) = validate_config_job_dates(config_job)

        self.datetime_start = datetime_start
        self.datetime_end = datetime_end

        try:
            create_download_dir(download_dir=DOWNLOAD_DIR)

            upload_response = self.work_process()

        except Exception as ex:
            self.logger.error(
                'Worker: Failed: Unexpected Error',
                extra={'error_exception': base_class_name(ex),
                       'error_details': get_exception_message(ex)})
            raise

        return upload_response

    def work_process(self):
        try:
            # Opens a file for appending.
            upload_json_file_name = self.UPLOAD_FILE_NAME + ".json.gz"

            tmp_json_file_path = \
                "{tmp_directory}/{tmp_json_file_name}".format(
                    tmp_directory=DOWNLOAD_DIR,
                    tmp_json_file_name=upload_json_file_name
                )

            if os.path.exists(tmp_json_file_path):
                self.logger.debug("Removing previous JSON File", extra={'file_path': tmp_json_file_path})
                os.remove(tmp_json_file_path)

            # GET client's account_ids
            self.fb_ad_accounts = self.fb_ad_accounts_get(self.credentials['access_token'])
            if not self.fb_ad_accounts or len(self.fb_ad_accounts) == 0:
                error_code = RequestsWorkerStatusCode.MOD_ERR_CLIENT_ACCOUNT_IDS_EMPTY
                self.logger.error(
                    error_desc(error_code),
                    extra={'client_id': self.client_id})

                raise RequestsWorkerClientError(
                    error_code=error_code
                )

            fb_ad_account_ids = sorted_nicely(list(self.fb_ad_accounts.keys()))
            self.logger.info(
                "FB Ad Accounts Actual",
                extra={
                    'fb_ad_account_ids': fb_ad_account_ids
                }
            )

            workers = min(self._MAX_WORKERS, len(self.fb_ad_accounts))

            # We will process accounts in threadpool
            # Each account task will put mapped report to queue upon finish
            # Writing to gzip file is on main thread by getting mapped reports
            # from queue.
            with gzip.open(filename=tmp_json_file_path, mode='ab') as gz_json_file_wb:
                line_count = 0

                with futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    # executor.submit schedule self.process_account to be executed
                    # for each account and returns a future representing this
                    # pending operation.
                    threads = []
                    for account_id, account in self.fb_ad_accounts.items():
                        self.logger.info(
                            f"Account '{account_id}': executor"
                        )
                        future = executor.submit(self.fb_ad_account_process, account_id, account)
                        threads.append(future)

                    # Each processed account will yield future
                    # every yield empty the queue which hold mapped reports
                    # from different threads
                    for future in futures.as_completed(threads):
                        for mapped_report in future.result():
                            line_count += 1
                            if line_count > 1:
                                gz_json_file_wb.write(bytes('\n', 'ascii'))

                            # Append mapped JSON row to JSON file
                            gz_json_file_wb.write(bytes(mapped_report, 'ascii'))
                            if line_count % 5000 == 0:
                                gz_json_file_wb.flush()
                                os.fsync(gz_json_file_wb)

            if os.path.exists(tmp_json_file_path):
                tmp_json_file_size = os.path.getsize(tmp_json_file_path)
                upload_response = (tmp_json_file_path, tmp_json_file_size, line_count)
            else:
                upload_response = (None, 0, 0)

            # self.fb_data_tracking[_GRANULARITY_DAY].pop("main_report")
            # self.fb_data_tracking[_GRANULARITY_HOUR].pop("main_report")
            # self.logger.info(
            #     "FB Data",
            #     extra=self.fb_data_tracking
            # )

        except (RequestsWorkerAuthenticationError, RequestsFortifiedAuthenticationError) as tmc_auth_ex:
            self.logger.error("Authentication: Failed", extra=tmc_auth_ex.to_dict())
            raise RequestsWorkerClientUnauthorizedError(
                error_message=tmc_auth_ex.error_message,
                error_details=tmc_auth_ex.error_details,
                error_origin=tmc_auth_ex.error_origin,
                error_request_curl=tmc_auth_ex.error_request_curl,
                errors=tmc_auth_ex)

        except (RequestsWorkerBaseError, RequestsFortifiedBaseError) as tmv_ex:
            extra = tmv_ex.to_dict()
            extra.update({'error_exception': base_class_name(tmv_ex)})
            self.logger.error('Worker: Failed', extra=extra)
            raise

        except Exception as ex:
            self.logger.error(
                'Worker: Failed: Unexpected Error',
                extra={'error_exception': base_class_name(ex),
                       'error_details': get_exception_message(ex)})
            raise

        return upload_response

    def fb_ad_accounts_get(self, access_token):
        """Facebook Ads: Get Ad Accounts

        Args:
            access_token:

        Returns:
            Dictionary of accounts

        """
        self.logger.debug("FB Ad Accounts")
        request_url = f'{FACEBOOK_GRAPH_API_URL}/me/adaccounts'

        (dict_accounts, cache_key) = self.cache.get(
            request_url=request_url, cache_group_name="FacebookAdAccounts")

        if dict_accounts is None:
            params_fb_graph_accounts = {
                'access_token': access_token,
                'limit': 500,
                'fields': 'account_id,name,currency,timezone_name'
            }

            try:
                response_accounts = self.fb_request(
                    request_method="GET",
                    request_url=request_url,
                    request_params=params_fb_graph_accounts,
                    request_retry=None,
                    request_headers=HEADER_CONTENT_TYPE_APP_JSON,
                    request_label="FB Ad Accounts"
                )
            except RequestsWorkerClientBadRequestError as client_bad_ex:
                error = client_bad_ex.errors.error_details.get('error', None)
                if error is not None:
                    error_message = error.get('message', None)
                    error_type = error.get('type', None)
                    if error_type == "OAuthException":
                        raise RequestsWorkerClientUnauthorizedError(error_message=error_message)
                raise
            except Exception:
                raise

            json_accounts = validate_json_response(
                response=response_accounts,
                request_label="FB Ad Accounts",
                request_curl=self.base_request.built_request_curl)

            self.logger.info(
                "FB Ad Accounts",
                extra=json_accounts
            )

            dict_accounts = dict([(x['account_id'], x) for x in json_accounts['data']])

            if dict_accounts is not None and cache_key is not None:
                self.cache.put(
                    cache_key=cache_key,
                    cache_value=dict_accounts,
                    expires_in=SECONDS_FOR_23_AND_HALF_HOURS,
                    cache_group_name="FacebookAdAccounts")

        self.logger.debug("FB Ad Accounts: Found", extra={"count": len(dict_accounts)})
        return dict_accounts

    def fb_request(
        self,
        request_method,
        request_url,
        request_params=None,
        request_data=None,
        request_retry=None,
        request_headers=None,
        request_label=None
    ):
        """
        Facebook requests

        :param request_method:
        :param request_url:
        :param request_params:
        :param request_data:
        :param request_retry:
        :param request_headers:
        :param request_label:
        :return:
        """
        request_data_decoded = None
        if request_data:
            request_data_decoded = urldecode(request_data)

        self.logger.debug(
            "Request",
            extra={
                'request_method': request_method,
                'request_url': request_url,
                'request_params': request_params,
                'request_data_decoded': request_data_decoded})

        response = None
        delay_secs = 2
        tries = 0

        while True:
            tries += 1
            if tries > 1:
                _request_label = f'{request_label}: Attempt {tries}'
            else:
                _request_label = request_label

            try:
                response = self.base_request.request(
                    request_method=request_method,
                    request_url=request_url,
                    request_params=request_params,
                    request_data=request_data,
                    request_retry=request_retry,
                    request_retry_excps_func=None,
                    request_headers=request_headers,
                    request_label=_request_label
                )
            except RequestsFortifiedClientError as client_bad_ex:
                if client_bad_ex.error_code != RequestsWorkerStatusCode.BAD_REQUEST:
                    raise

                # https://developers.facebook.com/docs/marketing-api/error-reference

                # 17 - User request limit reached
                # 102 - Session key invalid or no longer valid
                # 104 - Incorrect signature
                # 190 - Invalid OAuth 2.0 Access Token
                # 200 - Permission error

                if 'error' in client_bad_ex.error_details and 'code' in client_bad_ex.error_details['error']:
                    if int(client_bad_ex.error_details['error']['code']) in [17, 613]:
                        self.logger.warning(
                            "Error Code: {error_code}, "
                            "Error Message: {error_message}, "
                            "Retry Count: {retry_count}, "
                            "Retry Delay: {retry_delay}").format(
                                error_code=client_bad_ex.error_details['error']['code'],
                                error_message=client_bad_ex.error_details['error']['message'],
                                retry_count=tries,
                                retry_delay=delay_secs)
                        time.sleep(delay_secs)
                        continue

                    if int(client_bad_ex.error_details['error']['code']) in [102, 104, 190, 200]:
                        self.logger.error(
                            "FB Request failed",
                            extra={
                                'error_code': client_bad_ex.error_details['error']['code'],
                                'error_message': client_bad_ex.error_details['error']['message']})
                        raise RequestsWorkerClientUnauthorizedError(
                            error_message=client_bad_ex.error_details['error']['message'])
                raise RequestsWorkerClientBadRequestError(
                    error_message=client_bad_ex.error_message, errors=client_bad_ex)

            except (RequestsWorkerBaseError, RequestsFortifiedBaseError) as tmv_ex:
                raise
            except Exception as ex:
                raise RequestsWorkerSoftwareError(error_message=get_exception_message(ex))

            return response

    def fb_ad_account_process(self, account_id, account):
        """
        Facebook Process Account

        :param account_id:
        :param account:
        :return:
        """
        assert account_id
        assert account

        ad_account_name = account.get('name', None)

        self.logger.info(
            f"Account '{account_id}'",
            extra={
                'account_id': account_id,
                'account_name': ad_account_name
            }
        )

        account_apps = self.fb_account_apps_request(account_id=account_id)
        if not account_apps:
            self.logger.warning(
                f"Account '{account_id}': Advertisable Apps: None Provided",
                extra={'account_id': account_id})
            return []

        self.logger.info(
            f"Account '{account_id}': Advertisable Apps: Found",
            extra={
                'account_id': account_id,
                'account_apps_count': len(account_apps)
            }
        )

        report_data_rows = self.fb_get_ad_account_app_reports(account_id, account, account_apps)
        gc.collect()
        return report_data_rows


    def fb_account_apps_request(self, account_id):
        """
        FB Account Advertisable Applications

        :param account_id:
        :return:
        """
        assert account_id

        url_advertisable_apps = f'{FACEBOOK_GRAPH_API_URL}/act_{account_id}/advertisable_applications'
        params_advertisable_apps = {'access_token': self.credentials['access_token']}

        response_account_apps = self.fb_request(
            request_method="GET",
            request_url=url_advertisable_apps,
            request_params=params_advertisable_apps,
            request_retry=None,
            request_headers=HEADER_CONTENT_TYPE_APP_JSON,
            request_label=f"Account '{account_id}': Advertisable Apps: Request"
        )

        status_code = response_account_apps.status_code

        self.logger.debug(
            f"Account '{account_id}': Advertisable Apps: Status",
            extra={'status_code': status_code})

        if status_code != 200:
            if status_code in [400, 401, 403, 404, 406, 408]:
                raise RequestsWorkerClientError(
                    error_message=f"Account '{account_id}': Advertisable Apps: Status {status_code}",
                    error_code=status_code)
            else:
                raise RequestsWorkerSoftwareError(
                    error_message=f"Account '{account_id}': Advertisable Apps: Status {status_code}",
                    error_code=status_code)

        json_account_apps = response_account_apps.json()
        account_apps = dict([(x['id'], x) for x in json_account_apps['data']])

        return account_apps

    def fb_get_ad_account_app_reports(self, account_id, ad_account, account_apps):
        """
        Facebook Advertisable Apps Reports

        :param account_id:
        :param ad_account:
        :param account_apps:
        :return:
        """
        fb_report_data_rows = []
        if not account_apps:
            return fb_report_data_rows

        str_date_start = self.datetime_start.strftime('%Y-%m-%d')
        str_date_end = self.datetime_end.strftime('%Y-%m-%d')

        fb_insights_params = copy.copy(_FACEBOOK_INSIGHTS_PARAMS)
        fb_insights_params["access_token"] = self.credentials['access_token']

        fb_insights_params["time_range"]["since"] = str_date_start
        fb_insights_params["time_range"]["until"] = str_date_end

        fb_insights_params["action_attribution_windows"] = ['7d_click', '28d_click', '1d_view', '1d_click']
        fb_insights_params["fields"] = ",".join(str(e) for e in _FACEBOOK_INSIGHTS_FIELDS)

        self.logger.debug(
            "fb_insights_params",
            extra=fb_insights_params
        )

        fb_report_map_hourly = None
        if self.fb_data_tracking[_GRANULARITY_HOUR]["enabled"]:
            fb_report_map_hourly = self.fb_get_reports_data_hourly(
                account_id,
                ad_account,
                account_apps,
                fb_insights_params
            )

        fb_report_map_daily = None
        if self.fb_data_tracking[_GRANULARITY_DAY]["enabled"]:
            fb_report_map_daily = self.fb_get_reports_data_daily(
                account_id,
                ad_account,
                account_apps,
                fb_insights_params
            )

        if fb_report_map_daily and fb_report_map_hourly:
            fb_report_data_rows = self.fb_distribute_daily_to_hourly(
                fb_report_map_daily,
                fb_report_map_hourly
            )
        elif fb_report_map_hourly:
            fb_report_data_rows = self.fb_report_map_to_rows(fb_report_map_hourly)

        self.logger.info(
            f"Account '{account_id}': Reports Hourly",
            extra={
                "rows": len(fb_report_data_rows)
            }
        )

        return fb_report_data_rows

    def fb_get_reports_data_hourly(self, account_id, ad_account, account_apps, fb_insights_params):
        fb_insights_hourly_params = copy.copy(fb_insights_params)
        fb_insights_hourly_params["breakdowns"] = "hourly_stats_aggregated_by_advertiser_time_zone"

        return self.fb_get_reports_data(
            account_id,
            ad_account,
            account_apps,
            fb_insights_params=fb_insights_hourly_params,
            granularity=_GRANULARITY_HOUR
        )

    def fb_get_reports_data_daily(self, account_id, ad_account, account_apps, fb_insights_params):
        return self.fb_get_reports_data(
            account_id,
            ad_account,
            account_apps,
            fb_insights_params=copy.copy(fb_insights_params),
            granularity=_GRANULARITY_DAY
        )

    def fb_get_reports_data(
        self,
        account_id,
        ad_account,
        account_apps,
        fb_insights_params,
        granularity
    ):
        if not account_apps:
            return None

        # self.fb_data[granularity]["insights_params"] = fb_insights_params

        main_report_id = self.fb_reports_request(
            account_id,
            fb_insights_params,
            granularity
        )
        self.fb_data_tracking[granularity]["main_report_id"] = main_report_id
        self.logger.info(
            f"Account '{account_id}': Request '{granularity}' Report",
            extra={
                "main_report_id": main_report_id,
                "granularity": granularity
            }
        )
        if not main_report_id:
            self.logger.warning(
                f"Main '{granularity}' Report: Not Provided",
                extra={
                    "account_id": account_id,
                    "main_report_id": main_report_id,
                    "granularity": granularity
                })
            return None

        report_run_ids = self.fb_reports_jobs_request(
            account_id,
            main_report_id,
            fb_insights_params,
            granularity
        )
        # self.fb_data[granularity]["report_run_ids"] = report_run_ids
        self.logger.debug(
            f"Account '{account_id}': Gather Completed '{granularity}' Report Jobs",
            extra={
                "main_report_id": main_report_id,
                "report_run_ids": report_run_ids,
                "granularity": granularity
            })

        fb_reports = self.fb_reports_jobs_completion_request(
            account_id,
            report_run_ids,
            granularity
        )

        self.logger.info(
            f"Account '{account_id}': Reports '{granularity}': Completed",
            extra={
                'account_apps_count': len(account_apps),
                'main_report_id': main_report_id,
                'fb_reports': len(fb_reports)
            }
        )

        if not fb_reports:
            self.logger.warning(
                f"Account '{account_id}': Reports '{granularity}': None Found",
                extra={
                    'main_report_id': main_report_id,
                    'account_id': account_id
                }
            )
            return None

        if main_report_id not in fb_reports.keys():
            self.logger.warning(
                f"Account '{account_id}': Main Report '{main_report_id}' is missing",
                extra={
                    "main_report_id": main_report_id,
                    "report_ids": fb_reports.keys(),
                    "report_ids_count": len(fb_reports.keys())
                }
            )

        fb_campaigns = self.fb_map_campaigns_to_app_actions(
            account_id,
            account_apps,
            main_report_id,
            fb_reports,
            granularity
        )

        if not fb_campaigns:
            self.logger.warning(
                f"Account '{account_id}': Campaigns '{granularity}': None Found",
                extra={
                    'account_id': account_id,
                    'main_report_id': main_report_id
                }
            )
            return None

        self.logger.info(
            f"Account '{account_id}': Campaigns '{granularity}': Found",
            extra={
                'account_id': account_id,
                'main_report_id': main_report_id,
                'fb_campaigns': len(fb_campaigns)
            }
        )

        fb_report_map, report_row_count_total = self.fb_get_reports_mapping(
            account_id,
            ad_account,
            fb_campaigns,
            main_report_id,
            fb_reports,
            granularity=granularity
        )

        self.logger.info(
            f"Account '{account_id}': FB Report Daily Data: Found",
            extra={
                'main_report_id': main_report_id,
                'fb_report_data_rows': report_row_count_total
            }
        )

        return fb_report_map

    def fb_reports_request(self, account_id, fb_insights_params, granularity):
        """
        Facebook Reports

        :param fb_insights_params:
        :param account_id:
        :return: report_run_id
        """
        url_ad_account_insights = f'{FACEBOOK_GRAPH_API_URL}/act_{account_id}/insights'
        params_insights_encoded = urllib.parse.urlencode(fb_insights_params)

        response_async_export = self.fb_request(
            request_method="POST",
            request_url=url_ad_account_insights,
            request_data=params_insights_encoded,
            request_retry=None,
            request_headers=HEADER_CONTENT_TYPE_APP_JSON,
            request_label=f"Account '{account_id}': Reports '{granularity}': Request"
        )

        json_ad_account_insights = validate_json_response(
            response=response_async_export,
            request_curl=self.base_request.built_request_curl,
            request_label=f"Account '{account_id}': Reports '{granularity}': Validate"
        )

        return json_ad_account_insights.get("report_run_id", None)


    def fb_reports_jobs_request(
        self,
        account_id,
        main_report_id,
        fb_insights_params,
        granularity
    ):
        """
        Facebook Report Jobs: Request one job for each day between start and end dates.

        :param account_id:
        :param main_report_id:
        :param fb_insights_params:
        :return: report_run_ids list
        """
        assert account_id
        assert main_report_id
        assert fb_insights_params

        url_ad_account_insights = f'{FACEBOOK_GRAPH_API_URL}/act_{account_id}/insights'
        report_run_ids = [main_report_id]

        delta_day = dt.timedelta(days=1)
        cur_date = self.datetime_start - delta_day
        done = False
        while not done:
            cur_date = cur_date + delta_day

            if cur_date == self.datetime_end:
                done = True

            report_date = cur_date.strftime('%Y-%m-%d')

            fb_insights_params["time_range"]["since"] = report_date
            fb_insights_params["time_range"]["until"] = report_date

            params_insights_encoded = urllib.parse.urlencode(fb_insights_params)

            response_async_export = self.fb_request(
                request_method="POST",
                request_url=url_ad_account_insights,
                request_data=params_insights_encoded,
                request_retry=None,
                request_headers=HEADER_CONTENT_TYPE_APP_JSON,
                request_label=f"Account '{account_id}': Job Request: '{granularity}' Report Date '{report_date}': Request"
            )

            json_graph_insights = validate_json_response(
                response=response_async_export,
                request_label=f"Account '{account_id}': Job Request: '{granularity}' Report Date '{report_date}': Validate",
                request_curl=self.base_request.built_request_curl
            )

            report_run_id = json_graph_insights.get("report_run_id", None)

            if report_run_id is not None:
                report_run_ids.append(report_run_id)

        self.logger.info(
            f"Account '{account_id}': Request '{granularity}' Report Jobs",
            extra={
                'report_run_ids': report_run_ids,
                'account_id': account_id
            }
        )

        return report_run_ids

    def fb_reports_jobs_completion_request(self, account_id, report_run_ids, granularity):
        """
        Facebook Report Jobs: Wait until all jobs are completed.

        :param report_run_ids:
        :param account_id:
        :return: reports dictionary
        """
        reports = {}
        report_ids_completed = []
        sleep_secs = 2

        while len(report_run_ids) != len(report_ids_completed):
            for report_run_id in report_run_ids:
                if report_run_id in report_ids_completed:
                    continue

                url_fb_graph_async_rpt_status = f'{FACEBOOK_GRAPH_API_URL}/{report_run_id}'
                params_fb_graph_report_status = {'access_token': self.credentials['access_token']}

                response_async_status = self.fb_request(
                    request_method="GET",
                    request_url=url_fb_graph_async_rpt_status,
                    request_params=params_fb_graph_report_status,
                    request_retry=None,
                    request_headers=HEADER_CONTENT_TYPE_APP_JSON,
                    request_label=f"Account '{account_id}': Job Status: '{granularity}' Report '{report_run_id}': Request"
                )

                json_job_status = validate_json_response(
                    response=response_async_status,
                    request_label=f"Account '{account_id}': Job Status: '{granularity}' Report '{report_run_id}': Validate",
                    request_curl=self.base_request.built_request_curl
                )

                self.logger.debug(
                    f"Account '{account_id}': Job Status: '{granularity}' Report '{report_run_id}'",
                    extra={
                        'async_status': json_job_status['async_status'],
                        'async_percent_completion': json_job_status['async_percent_completion'],
                        'date_start': json_job_status['date_start'],
                        'date_stop': json_job_status['date_stop'],
                        'is_running': json_job_status.get('date_stop', None)
                    }
                )

                count_sleeps = 0
                while json_job_status['async_percent_completion'] < 100:
                    # Don't wait more than 60 seconds
                    if count_sleeps > 60:
                        break

                    count_sleeps += sleep_secs
                    time.sleep(sleep_secs)

                    async_status = json_job_status['async_status']
                    response_async_status = self.fb_request(
                        request_method="GET",
                        request_url=url_fb_graph_async_rpt_status,
                        request_params=params_fb_graph_report_status,
                        request_retry=None,
                        request_headers=HEADER_CONTENT_TYPE_APP_JSON,
                        request_label=f"Account '{account_id}': {async_status}: '{granularity}' Report '{report_run_id}': Request Status"
                    )

                    json_job_status = validate_json_response(
                        response=response_async_status,
                        request_label=f"Account '{account_id}': {async_status}: '{granularity}' Report '{report_run_id}': Validate",
                        request_curl=self.base_request.built_request_curl
                    )

                    self.logger.debug(
                        f"Account '{account_id}': {async_status}: '{granularity}' Report '{report_run_id}': Status",
                        extra={
                            'async_status': json_job_status['async_status'],
                            'async_percent_completion': json_job_status['async_percent_completion'],
                            'date_start': json_job_status['date_start'],
                            'date_stop': json_job_status['date_stop'],
                            'is_running': json_job_status.get('date_stop', None)
                        }
                    )

                if 'async_status' in json_job_status:
                    async_status = json_job_status['async_status']
                    if async_status == 'Job Completed':
                        report_ids_completed.append(report_run_id)
                        reports[report_run_id] = self.fb_reports_downloads_request(account_id, report_run_id, granularity)

                        self.logger.info(
                            f"Account '{account_id}': {async_status}: '{granularity}' Report '{report_run_id}'")

                    # elif async_status == "Job Not Started":
                    #     report_ids_completed.append(report_run_id)

                    # Failed job !!!
                    elif async_status == "Job Failed":
                        report_ids_completed.append(report_run_id)

                        self.logger.warning(
                            f"Account '{account_id}': {async_status}: '{granularity}' Report '{report_run_id}'")
                else:
                    self.logger.info(
                        f"Account '{account_id}': Job Completed: '{granularity}' Report '{report_run_id}': Async Status",
                        extra={'http_status_code': response_async_status.status_code})

        self.logger.info(
            f"Account '{account_id}': '{granularity}' Reports Completed",
            extra={
                'report_run_ids': list(reports.keys()),
                'account_id': account_id
            }
        )

        return reports

    def fb_reports_downloads_request(self, account_id, report_run_id, granularity):
        """
        Facebook Report Download

        :param account_id:
        :param report_run_id:
        :return:
        """
        url_fb_graph_async_rpt_insights = f'{FACEBOOK_GRAPH_API_URL}/{report_run_id}/insights'

        params_fb_graph_report_insights = {
            'access_token': self.credentials['access_token'],
            'limit': 1000
        }
        page_count = 0

        self.logger.debug(
            f"Account '{account_id}': Download '{granularity}' Report '{report_run_id}'"
        )

        reports = []
        while True:
            page_count += 1

            response_report = self.fb_request(
                request_method="GET",
                request_url=url_fb_graph_async_rpt_insights,
                request_params=params_fb_graph_report_insights,
                request_retry=None,
                request_headers=HEADER_CONTENT_TYPE_APP_JSON,
                request_label=f"Account '{account_id}': Download '{granularity}' Report '{report_run_id}': Page {page_count}: Request"
            )

            json_report = validate_json_response(
                response=response_report,
                request_curl=self.base_request.built_request_curl,
                request_label=f"Account '{account_id}': Download '{granularity}' Report '{report_run_id}': Page {page_count}: Validate"
            )

            if 'data' not in json_report:
                break

            assert isinstance(json_report['data'], list)
            reports.extend(json_report['data'])

            if 'paging' in json_report \
                    and 'next' in json_report['paging']:
                url_fb_graph_async_rpt_insights = json_report['paging']['next']
            else:
                break

        self.logger.debug(
            f"Account '{account_id}': Download '{granularity}' Report '{report_run_id}' : Finished",
            extra={'row_count': len(reports)})

        return reports

    def fb_get_reports_mapping(
        self,
        account_id,
        ad_account,
        fb_campaigns,
        main_report_id,
        fb_reports,
        granularity
    ):
        """
        :param account_id:
        :param ad_account:
        :param campaigns:
        :param main_report_id:
        :param fb_reports:
        :param granularity:
        :return:
        """
        fb_report_map = {}
        report_row_count_total = 0
        try:
            for report_id, report_data in fb_reports.items():
                # the main report is only used to map campaigns to sites
                if main_report_id == report_id:
                    continue

                if report_data is None:
                    continue

                report_row_count = 0
                for report_segment in report_data:
                    report_row_count += 1
                    fb_report_row_mapped = self.fb_get_report_mapping(
                        account_id,
                        ad_account,
                        fb_campaigns,
                        report_segment,
                        granularity
                    )

                    date = fb_report_row_mapped.get("date")
                    vendor_account_id = fb_report_row_mapped.get("vendor_account_id")
                    campaign_id = fb_report_row_mapped.get("campaign_id")
                    ad_id = fb_report_row_mapped.get("ad_id")
                    adset_id = fb_report_row_mapped.get("adset_id")
                    campaign_type = fb_report_row_mapped.get("campaign_type")
                    key = f"{date}:{vendor_account_id}:{campaign_id}:{ad_id}:{adset_id}:{campaign_type}"
                    hash_key = create_hash_key(key)

                    # self.fb_data[granularity]["hash_keys"][hash_key] = key

                    if fb_report_map.get(hash_key) is None:
                        fb_report_map[hash_key] = {}

                    hour = fb_report_row_mapped.get("hour")
                    if fb_report_map[hash_key].get(hour) is None:
                        fb_report_map[hash_key][hour] = {}
                    fb_report_map[hash_key][hour] = fb_report_row_mapped

                self.logger.debug(
                    f"Account '{account_id}': Report '{report_id}' : Mapped",
                    extra={'report_row_count': report_row_count}
                )
                report_row_count_total += report_row_count

        except Exception as ex:
            self.logger.error(
                "Report Mapping: Unexpected Error",
                extra={'error_exception': base_class_name(ex),
                       'error_details': get_exception_message(ex)})
            raise

        self.logger.debug(
            f"Account '{account_id}' : Mapped",
            extra={'report_row_count_total': report_row_count_total}
        )

        return fb_report_map, report_row_count_total

    def fb_get_report_mapping(
        self,
        account_id,
        ad_account,
        fb_campaigns,
        report_segment,
        granularity
    ):
        """
        :param account_id:
        :param ad_account:
        :param fb_campaigns:
        :param report_segment:
        :param granularity:
        :return:
        """
        ad_account_tz_name = ad_account.get("timezone_name", None)
        ad_account_currency = ad_account.get("currency", None)

        received_installs = 0
        received_installs_1d_view = 0
        received_installs_7d_click = 0
        received_installs_28d_click = 0
        received_impressions_complete = 0
        custom_action_events_cost = defaultdict(int)

        report_segment_actions = report_segment.get("actions", None)
        if report_segment_actions is not None and len(report_segment_actions) > 0:
            for action in report_segment_actions:
                action_type = action.get("action_type", None)
                if action_type is None:
                    continue

                if action_type == "mobile_app_install":
                    received_installs += safe_int(action["value"])

                    if "1d_view" in action:
                        received_installs_1d_view += safe_int(action["1d_view"])
                    if "7d_click" in action:
                        received_installs_7d_click += safe_int(action["7d_click"])
                    if "28d_click" in action:
                        received_installs_28d_click += safe_int(action["28d_click"])

                elif action_type == "video_p100_watched_actions":
                    received_impressions_complete += safe_int(action["value"])

                # calculate custom events cost
                elif action_type in _FACEBOOK_INSIGHTS_ACTIONS_MAPPING:
                    mapped_name_prefix = _FACEBOOK_INSIGHTS_ACTIONS_MAPPING[action_type]

                    custom_action_events_cost[mapped_name_prefix] += safe_int(action.get('value', 0))
                    custom_action_events_cost[mapped_name_prefix + '_1d_view'] += safe_int(action.get('1d_view', 0))
                    custom_action_events_cost[mapped_name_prefix + '_7d_click'] += safe_int(action.get('7d_click', 0))
                    custom_action_events_cost[mapped_name_prefix + '_28d_click'] += safe_int(action.get('28d_click', 0))

        received_revenue = 0.0
        received_revenue_1d_view = 0.0
        received_revenue_7d_click = 0.0
        received_revenue_28d_click = 0.0

        report_segment_action_values = report_segment.get("action_values", None)
        if report_segment_action_values is not None and len(report_segment_action_values) > 0:
            for action_value in report_segment_action_values:
                action_type = action_value.get("action_type", None)
                if action_type is None:
                    continue

                if action_type not in ['app_custom_event.fb_mobile_purchase']:
                    continue

                received_revenue += safe_float(action_value.get("value", 0.0))
                received_revenue_1d_view += safe_float(action_value.get("1d_view", 0.0))
                received_revenue_7d_click += safe_float(action_value.get("7d_click", 0.0))
                received_revenue_28d_click += safe_float(action_value.get("28d_click", 0.0))

        # TODO: How to parse unique_actions
        received_conversions = 0
        if "unique_actions" in report_segment:
            received_conversions = len(report_segment["unique_actions"])

        # By including the objective field to set campaign_type
        campaign_type = None
        campaign_objective = report_segment.get("objective", None)
        if campaign_objective is not None:
            if campaign_objective in ["MOBILE_APP_INSTALLS", "APP_INSTALLS"]:
                campaign_type = "acquisition"
            elif campaign_objective in ["MOBILE_APP_ENGAGEMENT", "LINK_CLICKS", "CONVERSIONS"]:
                campaign_type = "engagement"

        agency_id = None
        if self.agency_ids_map is not None and account_id in self.agency_ids_map:
            agency_id = self.agency_ids_map[account_id]

        campaign_platform = ""
        impression_device = ""
        # publisher_platform = ""
        # device_platform = ""
        if fb_campaigns and "campaign_id" in report_segment:
            fb_campaign_id = report_segment.get("campaign_id", None)
            fb_campaign_ids = list(fb_campaigns.keys())

            if fb_campaign_id and fb_campaign_id in fb_campaign_ids:
                fb_campaign = fb_campaigns[fb_campaign_id]
                if fb_campaign:
                    campaign_platform = fb_campaign.get("campaign_platform", "")
                    impression_device = fb_campaign.get("impression_device", "")
                    # publisher_platform = fb_campaign.get("publisher_platform", "")
                    # device_platform = fb_campaign.get("device_platform", "")

        return self.map_data_row(
            data_row=report_segment,
            config_job=self.config_job,
            data_supplemental={
                "account_id": safe_str(account_id),
                # "site_ref_id": safe_str(report_campaign.get('site_ref_id', None)),
                # "site_ref_type": safe_str(report_campaign.get('site_ref_type', None)),
                # "platform_ref": safe_str(report_campaign.get('platform_ref', None)),
                # "platform_type": safe_str(report_campaign.get('platform_type', None)),
                "timezone": safe_str(ad_account_tz_name),
                "campaign_type": safe_str(campaign_type),
                "campaign_platform": safe_str(campaign_platform),
                "impression_device": safe_str(impression_device),
                # "publisher_platform": safe_str(publisher_platform),
                # "device_platform": safe_str(device_platform),
                "campaign_objective": safe_str(campaign_objective),
                "agency_id": safe_int(agency_id),
                "received_impressions_complete": safe_int(received_impressions_complete),
                "received_installs": safe_int(received_installs),
                "received_installs_7d_click": safe_int(received_installs_7d_click),
                "received_installs_28d_click": safe_int(received_installs_28d_click),
                "received_installs_1d_view": safe_int(received_installs_1d_view),
                "received_conversions": safe_int(received_conversions),
                "cost_currency": safe_str(ad_account_currency),
                "received_revenue_currency": safe_str(ad_account_currency),
                "received_revenue": safe_float(received_revenue),
                "received_revenue_7d_click": safe_float(received_revenue_7d_click),
                "received_revenue_28d_click": safe_float(received_revenue_28d_click),
                "received_revenue_1d_view": safe_float(received_revenue_1d_view),
                "custom_action_events_cost": custom_action_events_cost,
                "granularity": granularity
            })

    def fb_map_campaigns_to_app_actions(
        self,
        account_id,
        account_apps,
        main_report_id,
        fb_reports,
        granularity
    ):
        """
        Map campaigns to actions of advertizable applications.
        The main report is only used to map campaigns to sites.

        :param account_id:
        :param account_apps:
        :param main_report_id:
        :param fb_reports:
        :param granularity:
        :return: campaigns to advertizable apps' actions
        """
        assert main_report_id
        assert fb_reports

        if main_report_id not in fb_reports.keys():
            self.logger.warning(
                f"Account '{account_id}': Main Report '{main_report_id}' is missing",
                extra={
                    "main_report_id": main_report_id,
                    "report_ids": fb_reports.keys(),
                    "report_ids_count": len(fb_reports.keys())
                }
            )

        assert main_report_id in fb_reports.keys()

        if not account_apps or len(account_apps) == 0:
            self.logger.warning(
                f"Account '{account_id}': Get Campaigns '{granularity}': No Advertizable Apps to Map Campaigns",
                extra={
                    "main_report_id": main_report_id
                }
            )
            return None

        main_report_source = granularity
        if fb_reports[main_report_id] is None or len(fb_reports[main_report_id]) == 0:
            self.logger.warning(
                f"Account '{account_id}': Get Campaigns '{granularity}': No Data in Main Report",
                extra={
                    "main_report_id": main_report_id
                }
            )

            main_report_source = _GRANULARITY_DAY if granularity == _GRANULARITY_HOUR else _GRANULARITY_HOUR
            main_report = self.fb_data_tracking[main_report_source].get("main_report", None)

            if main_report is None or len(main_report) == 0:
                return None

            fb_reports[main_report_id] = main_report

        self.fb_data_tracking[granularity]["main_report"] = fb_reports[main_report_id]
        self.fb_data_tracking[granularity]["main_report_source"] = main_report_source

        self.logger.info(
            f"Account '{account_id}': Get Campaigns '{granularity}': Main Report Data",
            extra={
                "main_report_id": main_report_id,
                "main_report_length": len(fb_reports[main_report_id])
            }
        )

        fb_campaigns = {}
        for report in fb_reports[main_report_id]:
            campaign_id = report.get("campaign_id", None)
            if not campaign_id:
                continue

            if campaign_id in fb_campaigns:
                continue

            fb_campaigns[campaign_id] = {}

            if "actions" in report:
                for action in report["actions"]:
                    if 'action_target_id' not in action:
                        continue  # ignore untrackable items

                    action_target_id = action['action_target_id']
                    connection_object = account_apps.get(action_target_id, None)
                    if not connection_object:
                        continue  # ignore untrackable items

                    website = connection_object.get('website')
                    native_app_store_ids = connection_object.get('native_app_store_ids')

                    self.logger.debug(
                        "connection_object",
                        extra = {
                            "object": dir(connection_object),
                            "website": website if website else None,
                            "native_app_store_ids_type": type(native_app_store_ids) if native_app_store_ids else None,
                            "native_app_store_ids": list(native_app_store_ids) if native_app_store_ids else None
                        }
                    )

                    fb_campaigns[campaign_id]["impression_device"] = action.get('impression_device', '').split('_')[0]
                    # fb_campaigns[campaign_id]["publisher_platform"] = action.get('publisher_platform', '').split('_')[0]
                    # fb_campaigns[campaign_id]["device_platform"] = action.get('device_platform', '').split('_')[0]

                    if website:
                        fb_campaigns[campaign_id]["campaign_platform"] = "web"

                    elif native_app_store_ids:
                        for platform in native_app_store_ids:
                            if native_app_store_ids[platform]:
                                fb_campaigns[campaign_id] = {"site_ref": safe_str(native_app_store_ids[platform])}
                                if platform == 1 or platform == 2 or platform == 3:
                                    fb_campaigns[campaign_id]["campaign_platform"] = "web"
                                elif platform == 4 or platform == 5:
                                    fb_campaigns[campaign_id]["campaign_platform"] = "ios"
                                elif platform == 6:
                                    fb_campaigns[campaign_id]["campaign_platform"] = "android"
                                break

        self.logger.info(
            f"Account '{account_id}': Get Campaigns '{granularity}'",
            extra={
                'main_report_id': main_report_id,
                'account_id': account_id,
                'fb_campaigns': fb_campaigns
            }
        )

        return fb_campaigns

    def fb_distribute_daily_to_hourly(self, fb_report_map_daily, fb_report_map_hourly):
        """
        Distribute unique daily values across hourly values
        :param fb_report_map_daily:
        :param fb_report_map_hourly:
        :return: array
        """
        # self.fb_hash_keys_decrypted(fb_report_map_daily, _GRANULARITY_DAY)
        # self.fb_hash_keys_decrypted(fb_report_map_hourly, _GRANULARITY_HOUR)

        for hash_key, day_map in fb_report_map_daily.items():
            hours_map = fb_report_map_hourly.get(hash_key, None)
            if not hours_map:
                continue

            received_clicks_unique = day_map[0].get("received_clicks_unique", 0)
            received_impressions_unique = day_map[0].get("received_impressions_unique", 0)

            if received_clicks_unique > 0 or received_impressions_unique > 0:
                # Continue until all unique values are completely distributed.
                while received_clicks_unique > 0 or received_impressions_unique > 0:
                    for hour in hours_map.keys():
                        hour_map = hours_map.pop(hour)

                        if received_clicks_unique > 0:
                            received_clicks_gross_hour = hour_map.get("received_clicks_gross", 0)
                            if received_clicks_gross_hour > 0:
                                received_clicks_unique_hour = hour_map.get("received_clicks_unique", 0)
                                # Unique value cannot be greater than Gross value.
                                if received_clicks_gross_hour > received_clicks_unique_hour:
                                    received_clicks_unique_hour += 1
                                    received_clicks_unique -= 1

                                    hour_map["received_clicks_unique"] = received_clicks_unique_hour

                        if received_impressions_unique > 0:
                            received_impressions_gross_hour = hour_map.get("received_impressions_gross", 0)
                            if received_impressions_gross_hour > 0:
                                received_impressions_unique_hour = hour_map.get("received_impressions_unique", 0)
                                # Unique value cannot be greater than Gross value.
                                if received_impressions_gross_hour > received_impressions_unique_hour:
                                    received_impressions_unique_hour += 1
                                    received_impressions_unique -= 1

                                    hour_map["received_impressions_unique"] = received_impressions_unique_hour

                        hours_map[hour] = hour_map

                        if received_clicks_unique == 0 and received_impressions_unique == 0:
                            break

            assert received_clicks_unique == 0
            assert received_impressions_unique == 0

        return self.fb_report_map_to_rows(fb_report_map_hourly)

    def fb_report_map_to_rows(self, fb_report_map):
        fb_report_rows = []
        for _, hours_map in fb_report_map.items():
            for _, hour_map in hours_map.items():
                fb_report_rows.append(json.dumps(hour_map))

        return fb_report_rows

    def map_data_row(
        self,
        data_row,
        config_job,
        latency_mins=None,
        unix_time_epoch_secs=None,
        raw_json=None,
        data_supplemental=None,
        data_date=None
    ):
        """Map Data Row

        Args:
            data_row:
            config_job:
            latency_mins:
            unix_time_epoch_secs:
            raw_json:
            data_supplemental:
            data_date:

        Returns:
            Data Row (dict)

        """
        granularity = data_supplemental.get('granularity', self._GRANULARITY)

        rdate = dt.datetime.strptime(data_row["date_start"], '%Y-%m-%d')
        rdate_yyyy_mm_dd = rdate.strftime('%Y-%m-%d')
        rhour = self.get_rhour(data_row) if granularity == _GRANULARITY_HOUR else 0

        # Parse campaign name params, example: 'Default Name - App installs - my_campaign:US1 agency_id:10'
        # Start with campaign, if found it's valid for adset and ad, if not try with adset, etc...
        # my_campaign_name = FacebookAdsWorker.extract_my_campaign(data_row)

        # vendor_account_id = data_supplemental.get('vendor_account_id', None)
        # site_ref_id = data_supplemental.get('site_ref_id', 0)
        # site_ref_type = data_supplemental.get('site_ref_type', None)
        # platform_ref = data_supplemental.get('platform_ref', None)
        # platform_type = data_supplemental.get('platform_type', None)
        timezone = data_supplemental.get('timezone', None)
        cost_currency = data_supplemental.get('cost_currency', None)
        campaign_type = data_supplemental.get('campaign_type', None)
        campaign_objective = data_supplemental.get('campaign_objective', None)
        campaign_platform = data_supplemental.get('campaign_platform', None)
        impression_device = data_supplemental.get('impression_device', None)
        # agency_id = data_supplemental.get('agency_id', None)

        received_installs_1d_view = data_supplemental.get('received_installs_1d_view', None)
        received_installs_1d_click = data_supplemental.get('received_installs_1d_click', None)
        received_installs_7d_click = data_supplemental.get('received_installs_7d_click', None)
        received_installs_28d_click = data_supplemental.get('received_installs_28d_click', None)

        received_impressions_complete = safe_int(data_supplemental["received_impressions_complete"])
        received_installs = safe_int(data_supplemental["received_installs"])
        received_conversions = safe_int(data_supplemental["received_conversions"])

        received_revenue = safe_float(data_supplemental.get('received_revenue', 0.0))
        received_revenue_1d_view = safe_float(data_supplemental.get('received_revenue_1d_view', 0.0))
        received_revenue_1d_click = safe_float(data_supplemental.get('received_revenue_1d_click', 0.0))
        received_revenue_7d_click = safe_float(data_supplemental.get('received_revenue_7d_click', 0.0))
        received_revenue_28d_click = safe_float(data_supplemental.get('received_revenue_28d_click', 0.0))

        received_impressions_gross = safe_int(data_row.get('impressions', None))
        received_impressions_unique = safe_int(data_row.get('reach', None))
        received_clicks_gross = safe_int(data_row.get('clicks', None))
        received_clicks_unique = safe_int(data_row.get('unique_clicks', None))
        received_spend = safe_float(data_row.get('spend', None))

        output_data_row = {
            "date": safe_str(rdate_yyyy_mm_dd),
            "hour": safe_int(rhour),
            "timezone": safe_str(timezone),
            "granularity": granularity,
            "campaign_type": safe_str(campaign_type),
            "campaign_objective": safe_str(campaign_objective),
            "campaign_platform": safe_str(campaign_platform),
            "impression_device": safe_str(impression_device),
            "campaign_id": safe_str(data_row.get("campaign_id", None)),
            "campaign_name": safe_str(data_row.get("campaign_name", None)),
            "adset_id": safe_str(data_row.get("adset_id", None)),
            "adset_name": safe_str(data_row.get("adset_name", None)),
            "ad_id": safe_str(data_row.get("ad_id", None)),
            "ad_name": safe_str(data_row.get("ad_name", None)),
            # "agency_id": safe_int(agency_id),
            "received_impressions_gross": received_impressions_gross,
            "received_impressions_unique": received_impressions_unique,
            "received_impressions_complete": safe_int(received_impressions_complete),
            "received_clicks_gross": received_clicks_gross,
            "received_clicks_unique": received_clicks_unique,
            "received_conversions": safe_int(received_conversions),
            "received_installs": safe_int(received_installs),

            "received_installs_1d_view": safe_int(received_installs_1d_view),
            "received_installs_1d_click": safe_int(received_installs_1d_click),
            "received_installs_7d_click": safe_int(received_installs_7d_click),
            "received_installs_28d_click": safe_int(received_installs_28d_click),

            "received_revenue": safe_float(received_revenue),
            "received_revenue_1d_view": safe_float(received_revenue_1d_view),
            "received_revenue_1d_click": safe_float(received_revenue_1d_click),
            "received_revenue_7d_click": safe_float(received_revenue_7d_click),
            "received_revenue_28d_click": safe_float(received_revenue_28d_click),
            "revenue_currency": safe_str(cost_currency),

            "cost": safe_float(received_spend),
            "cost_currency": safe_str(cost_currency)
        }

        # # Facebook custom_app actions # #: Field are calculated elsewhere and just added here.
        # Take extra precautions with them. Field names are define in: _FACEBOOK_INSIGHTS_ACTIONS_MAPPING
        custom_action_events_cost = data_supplemental.get('custom_action_events_cost', {})
        output_data_row.update(custom_action_events_cost)

        return output_data_row

    # Parse campaign name params, example: 'Default Name - App installs - my_campaign:US1 agency_id:10'
    # Start with campaign, if found it's valid for adset and ad, if not try with adset, etc...
    @staticmethod
    def extract_my_campaign(data_row):
        assert data_row

        my_campaign_name = None

        data_row_value = data_row.get("campaign_name", None)
        if data_row_value is not None:
            my_parameters_tags = FacebookAdsWorker.extract_my_parameters(data_row_value)
            if my_parameters_tags:
                my_campaign_name = my_parameters_tags.get('my_campaign')

        if not my_campaign_name:
            data_row_value = data_row.get("adset_name", None)
            if data_row_value is not None:
                my_parameters_tags = FacebookAdsWorker.extract_my_parameters(data_row_value)
                if my_parameters_tags:
                    my_campaign_name = my_parameters_tags.get('my_campaign')

        if not my_campaign_name:
            data_row_value = data_row.get("ad_name", None)
            if data_row_value is not None:
                my_parameters_tags = FacebookAdsWorker.extract_my_parameters(data_row_value)
                if my_parameters_tags:
                    my_campaign_name = my_parameters_tags.get('my_campaign')

        return my_campaign_name

    _ADDITIONAL_PARAMS_PATTERN = r"(\S+\s*:\s*\S+){1,}"
    _ADDITIONAL_PARAMATERS_KEYS = {
        'my_publisher': True,
        'my_site': True,
        'my_campaign': True,
        'my_adgroup': True,
        'my_ad': True,
        'my_keyword': True,
        'agency_id': True,
        'integration_id': True
    }

    @staticmethod
    def extract_my_parameters(name):
        """
        Process item (ad/adset/campaign) parameters from item name, validate if item should be skipped
        :param name: str
        :return: my_parameters_dict (dict)
        """
        assert name

        # Example: 'Default Name - App installs - my_campaign:US1 agency_id:10'
        my_parameters_dict = {}

        additional_params_matches = re.finditer(FacebookAdsWorker._ADDITIONAL_PARAMS_PATTERN, name)

        for _, key_val_pair_match in enumerate(additional_params_matches):

            key_val_pair_str = key_val_pair_match.group()
            # get key and value from key val string
            key_val_pair = key_val_pair_str.split(':', 1)

            if len(key_val_pair) == 2:
                key = key_val_pair[0].strip()
                if key in FacebookAdsWorker._ADDITIONAL_PARAMATERS_KEYS:
                    value = key_val_pair[1].strip()
                    my_parameters_dict[key] = value

        return my_parameters_dict

    def get_rhour(self, data_row):
        rhour_hourly_stats_raw = data_row.get("hourly_stats_aggregated_by_advertiser_time_zone", None)
        if rhour_hourly_stats_raw is None:
            raise RequestsWorkerSoftwareError(
                error_message="FB Graph API: Missing breakdowns 'hourly_stats_aggregated_by_advertiser_time_zone'"
            )

        rhour_hourly_stats_split = rhour_hourly_stats_raw.split(":")
        if not rhour_hourly_stats_split or len(rhour_hourly_stats_split) == 0:
            self.logger.warning(
                f"get_rhour: Invalid 'hourly_stats_aggregated_by_advertiser_time_zone': '{rhour_hourly_stats_raw}'"
            )
            return 0

        rhour_hourly_stats = rhour_hourly_stats_split[0]
        rhour_stripped = rhour_hourly_stats.lstrip("0")
        if rhour_stripped == "":
            rhour_stripped = "0"

        rhour = safe_int(rhour_stripped, -1)
        if rhour == -1:
            self.logger.warning(f"rhour: Invalid '{rhour_hourly_stats}'")
            rhour = 0

        return rhour


if __name__ == '__main__':
    # Create a factory class object
    worker_caller_factory = RequestsWorkerCallerFactory(worker_class=FacebookAdsWorker)
    # create the appropriate worker caller object
    worker_caller = worker_caller_factory.create()
    sys.exit(
        # Run this integration, while preparing input according to the running platform
        worker_caller.run_worker())
