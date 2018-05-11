#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  @copyright 2018, Jeff Tanner, jeff00seattle
#  @namespace facebook_ads_worker

PROFILE = [
]

REFERENCES = [
    'date',
    'hour',
    'timezone',
    'granularity',
    'cost_currency',
    'ad_name',
    'ad_id',
    'adset_name',
    'adset_id',
    'campaign_name',
    'campaign_id',
    'campaign_type',
    'campaign_platform',
    'impression_device'
]

UNMAPPED_IN_AA_METRICS = [
    'received_events_other_1d_view',
    'received_events_other_7d_click',
    'received_events_other_28d_click',
    'received_installs_1d_view',
    'received_installs_7d_click',
    'received_installs_28d_click',
    'received_revenue_1d_view',
    'received_revenue_7d_click',
    'received_revenue_28d_click',
    'received_impressions_complete',
    'received_conversions',
]
