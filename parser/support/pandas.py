#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  @copyright 2018, Jeff Tanner, jeff00seattle
#  @namespace facebook_ads_worker

from pprintpp import pprint
import pandas
import numpy as np

try:
    from fields import (PROFILE, REFERENCES, UNMAPPED_IN_AA_METRICS)
except ImportError as ex:
    from .fields import (PROFILE, REFERENCES, UNMAPPED_IN_AA_METRICS)


def pandasJsonNormalize(dataRaw):
    df = pandas.io.json.json_normalize(dataRaw)

    df_cols = df.columns.tolist()

    profile = PROFILE
    # for column in profile:
    #     if column not in df_cols:
    #         profile.pop(column)

    references = REFERENCES
    for column in references:
        if column not in df_cols:
            references.pop(column)

    columns = references

    for index, column in enumerate(columns, start=0):
        if column in df_cols:
            df_cols.insert(index, df_cols.pop(df_cols.index(column)))

    df = df[df_cols]

    order_by = ['date', 'hour']

    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    df = df.replace(np.nan, 0, regex=True)

    return df.sort_values(order_by), df_cols, profile, references

def pandasSummary(df, profile, references):
    not_group_by = ['hour', 'granularity']
    group_by = profile + references

    for _, item in enumerate(not_group_by, start=0):
        group_by.pop(group_by.index(item))

    df = df.drop(not_group_by, axis=1)
    df = df.groupby(group_by, as_index=False).sum()

    order_by = [
        'campaign_id',
        'ad_id',
        'date'
    ]

    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    # df = df.query("platform_type != 'web'")
    # order_by.pop(order_by.index('platform_type'))

    return df.sort_values(order_by)

def pandasGroupByCampaignsSummary(df, profile, references):

    all_columns = profile + references
    sum_columns = ['date', 'campaign_name', 'campaign_id' ]

    df_drop = []
    df_cols = df.columns.tolist()
    for _, column in enumerate(df_cols, start=0):
        if column in all_columns and column not in sum_columns:
            df_drop.append(column)
    df = df.drop(df_drop, axis=1)

    df_cols = df.columns.tolist()
    for index, column in enumerate(sum_columns, start=0):
        if column in df_cols:
            df_cols.insert(index, df_cols.pop(df_cols.index(column)))
    df = df[df_cols]

    # df = df.query("campaign_platform != 'web'")
    # df = df.drop(['platform_type'], axis=1)
    # profile.pop(profile.index('platform_type'))

    group_by = sum_columns
    df = df.groupby(group_by, as_index=False).sum()

    order_by = sum_columns
    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    return df.sort_values(order_by, ascending=[0, 1, 1])

def pandasGroupByAdAccounts(df, profile, references):

    all_columns = profile + references
    sum_columns = ['date', 'ad_name', 'ad_id']

    df_drop = []
    df_cols = df.columns.tolist()
    for _, column in enumerate(df_cols, start=0):
        if column in all_columns and column not in sum_columns:
            df_drop.append(column)
    df = df.drop(df_drop, axis=1)

    df_cols = df.columns.tolist()
    for index, column in enumerate(sum_columns, start=0):
        if column in df_cols:
            df_cols.insert(index, df_cols.pop(df_cols.index(column)))
    df = df[df_cols]

    # df = df.query("platform_type != 'web'")
    # df = df.drop(['platform_type'], axis=1)
    # profile.pop(profile.index('platform_type'))

    group_by = sum_columns
    df = df.groupby(group_by, as_index=False).sum()

    order_by = sum_columns
    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    return df.sort_values(order_by, ascending=[0, 1, 1])


def pandasGroupByHourlySummary(df, profile, references):

    all_columns = profile + references + UNMAPPED_IN_AA_METRICS
    sum_columns = ['date', 'hour', 'timezone']

    df_drop = []
    df_cols = df.columns.tolist()
    for _, column in enumerate(df_cols, start=0):
        if column in all_columns and column not in sum_columns:
            df_drop.append(column)
    df = df.drop(df_drop, axis=1)

    df_cols = df.columns.tolist()
    for index, column in enumerate(sum_columns, start=0):
        if column in df_cols:
            df_cols.insert(index, df_cols.pop(df_cols.index(column)))
    df = df[df_cols]

    # df = df.query("platform_type != 'web'")
    # df = df.drop(['platform_type'], axis=1)
    # profile.pop(profile.index('platform_type'))

    group_by = sum_columns
    df = df.groupby(group_by, as_index=False).sum()

    order_by = sum_columns
    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    return df.sort_values(order_by)


def pandasGroupByDailySummary(df, profile, references, platformType):

    all_columns = profile + references + UNMAPPED_IN_AA_METRICS
    sum_columns = ['date', 'timezone']

    df_drop = []
    df_cols = df.columns.tolist()
    for _, column in enumerate(df_cols, start=0):
        if column in all_columns and column not in sum_columns:
            df_drop.append(column)
    df = df.drop(df_drop, axis=1)

    df_cols = df.columns.tolist()
    for index, column in enumerate(sum_columns, start=0):
        if column in df_cols:
            df_cols.insert(index, df_cols.pop(df_cols.index(column)))
    df = df[df_cols]

    # if platformType is not None:
    #     df = df.query(f"platform_type == '{platformType}'")

    group_by = sum_columns
    df = df.groupby(group_by, as_index=False).sum()

    order_by = sum_columns
    for item in order_by:
        if item not in df.columns.tolist():
            order_by.pop(item)

    return df.sort_values(order_by)