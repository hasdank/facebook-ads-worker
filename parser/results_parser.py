#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  @copyright 2018, Jeff Tanner, jeff00seattle
#  @namespace facebook_ads_worker

import os
import sys
import getopt
from pprintpp import pprint

if __name__ == '__main__':
    from support import  (
        parseGzipNDJSON,
        parseNDJSON,

        pandasJsonNormalize,
        pandasSummary,
        pandasGroupByCampaignsSummary,
        pandasGroupByAdAccounts,
        pandasGroupByHourlySummary,
        pandasGroupByDailySummary,

        exportResultsCSV,
        exportResultsHTML,
        exportResultsTEX,
    )
else:
    from .support import  (
        parseGzipNDJSON,
        parseNDJSON,

        pandasJsonNormalize,
        pandasSummary,
        pandasGroupByCampaignsSummary,
        pandasGroupByAdAccounts,
        pandasGroupByHourlySummary,
        pandasGroupByDailySummary,

        exportResultsCSV,
        exportResultsHTML,
        exportResultsTEX,
    )


def main(argv):
    generateCSV=False
    generateHTML=False
    generateTEX=False

    verbose=False
    dataFilePath = ''
    try:
        opts, args = getopt.getopt(argv, 'vhd:', ['verbose', 'help', 'data=', 'csv', 'html', 'tex'])
    except getopt.GetoptError:
        print(f"{os.path.basename(__file__)} --data <datafile>")
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(f"{os.path.basename(__file__)} --data <datafile>")
            sys.exit()
        elif opt in ("-v", "--verbose"):
            verbose = True
        elif opt in ("--csv"):
            generateCSV = True
        elif opt in ("--html"):
            generateHTML = True
        elif opt in ("--tex"):
            generateTEX = True
        elif opt in ("-d", "--data"):
            dataFilePath = arg

    if verbose:
        pprint({
            "generateCSV": generateCSV,
            "generateHTML": generateHTML,
            "generateTEX": generateTEX,
        })

    if not os.path.isfile(dataFilePath):
        print(f"Results: File is missing: {dataFilePath}")
        sys.exit(1)
    if not os.access(dataFilePath, os.R_OK):
        print(f"Results: File is not readable: {dataFilePath}")
        sys.exit(1)

    if not generateCSV and not generateHTML and not generateTEX:
        generateCSV = True

    if dataFilePath.endswith('.gz'):
        dataRaw, num_lines = parseGzipNDJSON(dataFilePath)
    else:
        dataRaw, num_lines = parseNDJSON(dataFilePath)

    pprint(f"Results Data: {dataFilePath}, num lines: {num_lines}")

    if num_lines == 0 or not dataRaw:
        pprint("No results")
        sys.exit(0)

    dfNormalized, df_cols, profile, references = pandasJsonNormalize(dataRaw)
    if generateCSV:
        exportResultsCSV(dataFilePath, "normalized", dfNormalized)
    if generateHTML:
        exportResultsHTML(dataFilePath, "normalized", dfNormalized)
    if generateTEX:
        exportResultsTEX(dataFilePath, "normalized", dfNormalized)

    dfSummary = pandasSummary(dfNormalized, profile, references)
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary", dfSummary)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary", dfSummary)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary", dfSummary)

    dfDailySummary = pandasGroupByDailySummary(dfNormalized, profile, references, None)
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.daily", dfDailySummary)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.daily", dfDailySummary)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.daily", dfDailySummary)

    dfDailySummaryIOS = pandasGroupByDailySummary(dfNormalized, profile, references, "ios")
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.daily.ios", dfDailySummaryIOS)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.daily.ios", dfDailySummaryIOS)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.daily.ios", dfDailySummaryIOS)

    dfDailySummaryAndroid = pandasGroupByDailySummary(dfNormalized, profile, references, "android")
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.daily.android", dfDailySummaryAndroid)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.daily.android", dfDailySummaryAndroid)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.daily.android", dfDailySummaryAndroid)

    dfCampaignsSummary =  pandasGroupByCampaignsSummary(dfNormalized, profile, references)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.campaigns", dfCampaignsSummary)
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.campaigns", dfCampaignsSummary)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.campaigns", dfCampaignsSummary)

    dfAdAccounts =  pandasGroupByAdAccounts(dfNormalized, profile, references)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.ad_accounts", dfAdAccounts)
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.ad_accounts", dfAdAccounts)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.ad_accounts", dfAdAccounts)

    dfHourlySummary =  pandasGroupByHourlySummary(dfNormalized, profile, references)
    if generateHTML:
        exportResultsHTML(dataFilePath, "summary.hourly", dfHourlySummary)
    if generateCSV:
        exportResultsCSV(dataFilePath, "summary.hourly", dfHourlySummary)
    if generateTEX:
        exportResultsTEX(dataFilePath, "summary.hourly", dfHourlySummary)


if __name__ == "__main__":
   main(sys.argv[1:])
