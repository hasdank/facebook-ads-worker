# facebook-ads-worker
Facebook Ads Worker using Facebook Marketing and Insights APIs

### ***Work In Progress***
+ Documentation
  + Configuration
  + Results Parsing Tool

Code is complete, but documentation is required.

## Introduction

The following effort will be based on the work:
+ https://github.com/jeff00seattle/facebook-api-scripts

This code was built off these personally developed python projects:
+ https://github.com/jeff00seattle/logging-fortified
+ https://github.com/jeff00seattle/requests-fortified
+ https://github.com/jeff00seattle/requests-worker

Which are also in the process of being hosted on pypi.org respectively:
+ https://pypi.org/project/logging-fortified/
+ https://pypi.org/project/requests-fortified/
+ https://pypi.org/project/requests-worker/

## Configure Request

```json
{
  "job": {
    "start_date": "2018-05-06",
    "end_date": "2018-05-06"
  },
  "credentials": {
    "access_token": "[FACEBOOK ACCESS TOKEN]"
  },
  "upload": {
    "s3_url": "",
    "timeout": 1200
  },
  "logger": {
    "format": "json",
    "level": "debug"
  }
}
```

## Run Worker

```bash
$ make local-run

{"asctime": "2018-05-10 17:26:22 -0700", "levelname": "INFO", "name": "requests_worker", "version": "0.2.2", "message": "Download File: JSON", "upload_file_path": "/Users/jefft/github/jeff00seattle/facebook_ads_worker/tmp/facebook_ads.json.gz", "upload_file_size": "178.58 KB", "upload_row_count": 11532}
{"asctime": "2018-05-10 17:26:22 -0700", "levelname": "INFO", "name": "requests_worker", "version": "0.2.2", "message": "Run: Completed", "exit_code": 0, "exit_desc": "Successfully completed", "exit_name": "Success", "run_time_msec": 186733}
{"asctime": "2018-05-10 17:26:22 -0700", "levelname": "INFO", "name": "requests_worker", "version": "0.2.2", "message": "Facebook Ads Worker: Success", "exit_code": 0, "exit_desc": "Successfully completed", "exit_name": "Success", "run_time_msec": 186733}
{"asctime": "2018-05-10 17:26:22 -0700", "levelname": "INFO", "name": "requests_worker.requests_worker_factory", "version": "0.2.2", "message": "Completed: Success", "exit_code": 0, "exit_desc": "Successfully completed", "exit_name": "Success", "run_time_msec": 186733}
{"asctime": "2018-05-10 17:26:22 -0700", "levelname": "INFO", "name": "requests_worker.requests_worker_factory", "version": "0.2.2", "message": "Finished", "Exit Code": 0}
```

## Results Parsing

### Reports

```bash
tmp
├── facebook_ads.json.gz
└── reports
    ├── facebook_ads.full.csv
    ├── facebook_ads.full.html
    ├── facebook_ads.summary.campaigns.html
    ├── facebook_ads.summary.csv
    ├── facebook_ads.summary.daily.android.html
    ├── facebook_ads.summary.daily.ios.html
    ├── facebook_ads.summary.hourly.html
    ├── facebook_ads.summary.html
    └── facebook_ads.summary.sites.html
```

### Python Script `results_parser.py`

#### Install Requirements

```bash
$ brew install cairo pango gdk-pixbuf libffi
$ python3 -m install weasyprint
$ python3 -m install pandas
$ python3 -m install jinja2
$ python3 -m install numpy
```

#### Run

```bash
$ make results
======================================================
results tmp/facebook_ads.json.gz
======================================================
-rw-r--r--  1 jefft  staff  182868 May 10 17:26 tmp/facebook_ads.json.gz
'Results Data: tmp/facebook_ads.json.gz, num lines: 11532'
'Results CSV: tmp/reports/facebook_ads.normalized.csv'
'Results HTML: tmp/reports/facebook_ads.normalized.html'
'Results CSV: tmp/reports/facebook_ads.summary.csv'
'Results HTML: tmp/reports/facebook_ads.summary.html'
'Results CSV: tmp/reports/facebook_ads.summary.daily.csv'
'Results HTML: tmp/reports/facebook_ads.summary.daily.html'
'Results CSV: tmp/reports/facebook_ads.summary.daily.ios.csv'
'Results HTML: tmp/reports/facebook_ads.summary.daily.ios.html'
'Results CSV: tmp/reports/facebook_ads.summary.daily.android.csv'
'Results HTML: tmp/reports/facebook_ads.summary.daily.android.html'
'Results HTML: tmp/reports/facebook_ads.summary.campaigns.html'
'Results CSV: tmp/reports/facebook_ads.summary.campaigns.csv'
'Results HTML: tmp/reports/facebook_ads.summary.ad_accounts.html'
'Results CSV: tmp/reports/facebook_ads.summary.ad_accounts.csv'
'Results HTML: tmp/reports/facebook_ads.summary.hourly.html'
'Results CSV: tmp/reports/facebook_ads.summary.hourly.csv'
-rw-r--r--  1 jefft  staff  6081764 May 10 17:26 tmp/reports/facebook_ads.normalized.csv
-rw-r--r--  1 jefft  staff   314015 May 10 17:27 tmp/reports/facebook_ads.summary.ad_accounts.csv
-rw-r--r--  1 jefft  staff     9384 May 10 17:27 tmp/reports/facebook_ads.summary.campaigns.csv
-rw-r--r--  1 jefft  staff   452653 May 10 17:27 tmp/reports/facebook_ads.summary.csv
-rw-r--r--  1 jefft  staff      817 May 10 17:27 tmp/reports/facebook_ads.summary.daily.android.csv
-rw-r--r--  1 jefft  staff      817 May 10 17:27 tmp/reports/facebook_ads.summary.daily.csv
-rw-r--r--  1 jefft  staff      817 May 10 17:27 tmp/reports/facebook_ads.summary.daily.ios.csv
-rw-r--r--  1 jefft  staff     3999 May 10 17:27 tmp/reports/facebook_ads.summary.hourly.csv
-rw-r--r--  1 jefft  staff  12348392 May 10 17:27 tmp/reports/facebook_ads.normalized.html
-rw-r--r--  1 jefft  staff    636064 May 10 17:27 tmp/reports/facebook_ads.summary.ad_accounts.html
-rw-r--r--  1 jefft  staff     29722 May 10 17:27 tmp/reports/facebook_ads.summary.campaigns.html
-rw-r--r--  1 jefft  staff      2452 May 10 17:27 tmp/reports/facebook_ads.summary.daily.android.html
-rw-r--r--  1 jefft  staff      2452 May 10 17:27 tmp/reports/facebook_ads.summary.daily.html
-rw-r--r--  1 jefft  staff      2452 May 10 17:27 tmp/reports/facebook_ads.summary.daily.ios.html
-rw-r--r--  1 jefft  staff     13781 May 10 17:27 tmp/reports/facebook_ads.summary.hourly.html
-rw-r--r--  1 jefft  staff    884466 May 10 17:27 tmp/reports/facebook_ads.summary.html
```

#### `tmp/reports/facebook_ads.normalized.csv`

Contents of this file is a mapping from raw JSON response to CSV.

All of the following columns are the actual values mapped from values returned from collector:
* cost
* received_clicks_gross
* received_clicks_unique
* received_conversions
* received_events_other
* received_events_other_1d_view
* received_events_other_28d_click
* received_events_other_7d_click
* received_impressions_complete
* received_impressions_gross
* received_impressions_unique
* received_installs
* received_installs_1d_view
* received_installs_28d_click
* received_installs_7d_click
* received_revenue
* received_revenue_1d_view
* received_revenue_28d_click
* received_revenue_7d_click

```csv
date,hour,timezone,granularity,cost_currency,ad_name,ad_id,adset_name,adset_id,campaign_name,campaign_id,campaign_type,campaign_platform,impression_device,campaign_objective,cost,received_clicks_gross,received_clicks_unique,received_conversions,received_events_checkout_initiated,received_events_checkout_initiated_1d_view,received_events_checkout_initiated_28d_click,received_events_checkout_initiated_7d_click,received_events_other,received_events_other_1d_view,received_events_other_28d_click,received_events_other_7d_click,received_events_purchase,received_events_purchase_1d_view,received_events_purchase_28d_click,received_events_purchase_7d_click,received_events_registration,received_events_registration_1d_view,received_events_registration_28d_click,received_events_registration_7d_click,received_impressions_complete,received_impressions_gross,received_impressions_unique,received_installs,received_installs_1d_click,received_installs_1d_view,received_installs_28d_click,received_installs_7d_click,received_revenue,received_revenue_1d_click,received_revenue_1d_view,received_revenue_28d_click,received_revenue_7d_click,revenue_currency
2018-05-06,0,America/Los_Angeles,hour,USD,"Post: ""Next week the tavern will be decked out in its...""",6091490082247,"Post: ""Next week the tavern will be decked out in its...""",6091490082047,"Post: ""Next week the tavern will be decked out in its...""",6091490080447,,,,POST_ENGAGEMENT,0.46,2,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,167,1,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00,USD
2018-05-06,0,America/Los_Angeles,hour,USD,"Post: ""Looking for a new deck to play? Try out Charged...""",6091656546247,"Post: ""Looking for a new deck to play? Try out Charged...""",6091656546047,"Post: ""Looking for a new deck to play? Try out Charged...""",6091656544647,,,,POST_ENGAGEMENT,0.16,1,1,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,68,1,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00,USD
2018-05-06,0,America/Los_Angeles,hour,USD,RESIZE_rexxar1200x628,23842735695970684,iOS | US | iPhone | 1% LAL,23842735694610684,FB | HS | Display | Evergreen | Mobile | NA | iOS,23842735694550684,acquisition,,,APP_INSTALLS,0.08,0,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,6,6,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00,USD
2018-05-06,0,America/Los_Angeles,hour,USD,RESIZE_jaina1200x628,23842735695060684,iOS | US | iPhone | 1% LAL,23842735694610684,FB | HS | Display | Evergreen | Mobile | NA | iOS,23842735694550684,acquisition,,,APP_INSTALLS,0.45,0,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,29,29,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00,USD
2018-05-06,0,America/Los_Angeles,hour,USD,HS_SEPT_Resizing1200x628_Spellbender,23842735735450684,iOS | US | Android Phone | 1% LAL,23842735698820684,FB | HS | Display | Evergreen | Mobile | NA | Android,23842735698750684,acquisition,,,APP_INSTALLS,2.10,0,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,89,89,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00,USD
```

| date       | hour | timezone            | granularity | cost_currency | ad_name                                                    | ad_id             | adset_name                                                 | adset_id          | campaign_name                                              | campaign_id       | campaign_type | campaign_platform | impression_device | campaign_objective | cost | received_clicks_gross | received_clicks_unique | received_conversions | received_events_checkout_initiated | received_events_checkout_initiated_1d_view | received_events_checkout_initiated_28d_click | received_events_checkout_initiated_7d_click | received_events_other | received_events_other_1d_view | received_events_other_28d_click | received_events_other_7d_click | received_events_purchase | received_events_purchase_1d_view | received_events_purchase_28d_click | received_events_purchase_7d_click | received_events_registration | received_events_registration_1d_view | received_events_registration_28d_click | received_events_registration_7d_click | received_impressions_complete | received_impressions_gross | received_impressions_unique | received_installs | received_installs_1d_click | received_installs_1d_view | received_installs_28d_click | received_installs_7d_click | received_revenue | received_revenue_1d_click | received_revenue_1d_view | received_revenue_28d_click | received_revenue_7d_click | revenue_currency |
|------------|------|---------------------|-------------|---------------|------------------------------------------------------------|-------------------|------------------------------------------------------------|-------------------|------------------------------------------------------------|-------------------|---------------|-------------------|-------------------|--------------------|------|-----------------------|------------------------|----------------------|------------------------------------|--------------------------------------------|----------------------------------------------|---------------------------------------------|-----------------------|-------------------------------|---------------------------------|--------------------------------|--------------------------|----------------------------------|------------------------------------|-----------------------------------|------------------------------|--------------------------------------|----------------------------------------|---------------------------------------|-------------------------------|----------------------------|-----------------------------|-------------------|----------------------------|---------------------------|-----------------------------|----------------------------|------------------|---------------------------|--------------------------|----------------------------|---------------------------|------------------|
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | Post: "Next week the tavern will be decked out in its..."  | 6091490082247     | Post: "Next week the tavern will be decked out in its..."  | 6091490082047     | Post: "Next week the tavern will be decked out in its..."  | 6091490080447     |               |                   |                   | POST_ENGAGEMENT    | 0.46 | 2                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 167                        | 1                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | Post: "Looking for a new deck to play? Try out Charged..." | 6091656546247     | Post: "Looking for a new deck to play? Try out Charged..." | 6091656546047     | Post: "Looking for a new deck to play? Try out Charged..." | 6091656544647     |               |                   |                   | POST_ENGAGEMENT    | 0.16 | 1                     | 1                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 68                         | 1                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | RESIZE_rexxar1200x628                                      | 23842735695970684 | iOS | US | iPhone | 1% LAL                                 | 23842735694610684 | FB | HS | Display | Evergreen | Mobile | NA | iOS          | 23842735694550684 | acquisition   |                   |                   | APP_INSTALLS       | 0.08 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 6                          | 6                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | RESIZE_jaina1200x628                                       | 23842735695060684 | iOS | US | iPhone | 1% LAL                                 | 23842735694610684 | FB | HS | Display | Evergreen | Mobile | NA | iOS          | 23842735694550684 | acquisition   |                   |                   | APP_INSTALLS       | 0.45 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 29                         | 29                          | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | HS_SEPT_Resizing1200x628_Spellbender                       | 23842735735450684 | iOS | US | Android Phone | 1% LAL                          | 23842735698820684 | FB | HS | Display | Evergreen | Mobile | NA | Android      | 23842735698750684 | acquisition   |                   |                   | APP_INSTALLS       | 2.10 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 89                         | 89                          | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |


#### `tmp/[COLLECTOR].summary.csv`

Contents of this file is a sum of JSON responses to CSV.

Grouped by
* date
* ad_id


```csv
date,timezone,cost_currency,ad_name,ad_id,adset_name,adset_id,campaign_name,campaign_id,campaign_type,campaign_platform,impression_device,cost,received_clicks_gross,received_clicks_unique,received_conversions,received_events_checkout_initiated,received_events_checkout_initiated_1d_view,received_events_checkout_initiated_28d_click,received_events_checkout_initiated_7d_click,received_events_other,received_events_other_1d_view,received_events_other_28d_click,received_events_other_7d_click,received_events_purchase,received_events_purchase_1d_view,received_events_purchase_28d_click,received_events_purchase_7d_click,received_events_registration,received_events_registration_1d_view,received_events_registration_28d_click,received_events_registration_7d_click,received_impressions_complete,received_impressions_gross,received_impressions_unique,received_installs,received_installs_1d_click,received_installs_1d_view,received_installs_28d_click,received_installs_7d_click,received_revenue,received_revenue_1d_click,received_revenue_1d_view,received_revenue_28d_click,received_revenue_7d_click
2018-05-06,America/Los_Angeles,USD,810026_OW_US_EG_Social_Desktop_P_1200x628_SS_3PD_BHV_Adult Swim_CPP_Newsfeed_CPM_1x1 Click & Imp_Facebook_Facebook_March Ana Static Copy 13,23842741609950072,Newsfeed | Adult Swim | US | UA,23842741603960072,FB | OW | Display | Evergreen | Desktop | NA,23842599438570072,engagement,,,86.05,22,20,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,7021,6722,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00
2018-05-06,America/Los_Angeles,USD,810026_OW_US_EG_Social_Desktop_P_1200x628_SS_3PD_BHV_Cartoons_CPP_Newsfeed_CPM_1x1 Click & Imp_Facebook_Facebook_March Ana Static Copy 13,23842741624180072,Newsfeed | Cartoon | US | UA,23842741624170072,FB | OW | Display | Evergreen | Desktop | NA,23842599438570072,engagement,,,8.87,4,4,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,684,673,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00
2018-05-06,America/Los_Angeles,USD,810026_OW_US_EG_Social_Desktop_P_1200x628_SS_3PD_BHV_FPS_CPP_Newsfeed_CPM_1x1 Click & Imp_Facebook_Facebook_Widowmaker2 Sept Static Copy 20,23842743029460072,Newsfeed | FPS | US | UA,23842743029380072,FB | OW | Display | Evergreen | Desktop | NA,23842599438570072,engagement,,,22.99,13,12,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,1313,1271,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00
2018-05-06,America/Los_Angeles,USD,810026_OW_US_EG_Social_Desktop_P_1200x628_SS_3PD_BHV_Mobile Games_CPP_Newsfeed_CPM_1x1 Click & Imp_Facebook_Facebook_March Hanzo Static Copy 1,23842743034740072,Newsfeed | Mobile Games | US | UA,23842743034720072,FB | OW | Display | Evergreen | Desktop | NA,23842599438570072,engagement,,,0.00,0,0,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,1,1,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00
2018-05-06,America/Los_Angeles,USD,810026_OW_US_Promo_Banner_Desktop_P_1200x628_SS_3PD_BHV_Adult Swim_CPP_Newsfeed_dCPM_1x1 Click & Imp_Facebook_Facebook_OW-UA-JAN18-BASTION.COPY.Join.35.Million-0-Stat-1200X628-0,23842764324990072,Newsfeed | Adult Swim | US | UA,23842741603960072,FB | OW | Display | Evergreen | Desktop | NA,23842599438570072,engagement,,,7.28,1,1,0,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0.00,0,480,464,0,0,0,0,0,0.00,0.00,0.00,0.00,0.00

```

| date       | hour | timezone            | granularity | cost_currency | ad_name                                                    | ad_id             | adset_name                                                 | adset_id          | campaign_name                                              | campaign_id       | campaign_type | campaign_platform | impression_device | campaign_objective | cost | received_clicks_gross | received_clicks_unique | received_conversions | received_events_checkout_initiated | received_events_checkout_initiated_1d_view | received_events_checkout_initiated_28d_click | received_events_checkout_initiated_7d_click | received_events_other | received_events_other_1d_view | received_events_other_28d_click | received_events_other_7d_click | received_events_purchase | received_events_purchase_1d_view | received_events_purchase_28d_click | received_events_purchase_7d_click | received_events_registration | received_events_registration_1d_view | received_events_registration_28d_click | received_events_registration_7d_click | received_impressions_complete | received_impressions_gross | received_impressions_unique | received_installs | received_installs_1d_click | received_installs_1d_view | received_installs_28d_click | received_installs_7d_click | received_revenue | received_revenue_1d_click | received_revenue_1d_view | received_revenue_28d_click | received_revenue_7d_click | revenue_currency |
|------------|------|---------------------|-------------|---------------|------------------------------------------------------------|-------------------|------------------------------------------------------------|-------------------|------------------------------------------------------------|-------------------|---------------|-------------------|-------------------|--------------------|------|-----------------------|------------------------|----------------------|------------------------------------|--------------------------------------------|----------------------------------------------|---------------------------------------------|-----------------------|-------------------------------|---------------------------------|--------------------------------|--------------------------|----------------------------------|------------------------------------|-----------------------------------|------------------------------|--------------------------------------|----------------------------------------|---------------------------------------|-------------------------------|----------------------------|-----------------------------|-------------------|----------------------------|---------------------------|-----------------------------|----------------------------|------------------|---------------------------|--------------------------|----------------------------|---------------------------|------------------|
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | Post: "Next week the tavern will be decked out in its..."  | 6091490082247     | Post: "Next week the tavern will be decked out in its..."  | 6091490082047     | Post: "Next week the tavern will be decked out in its..."  | 6091490080447     |               |                   |                   | POST_ENGAGEMENT    | 0.46 | 2                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 167                        | 1                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | Post: "Looking for a new deck to play? Try out Charged..." | 6091656546247     | Post: "Looking for a new deck to play? Try out Charged..." | 6091656546047     | Post: "Looking for a new deck to play? Try out Charged..." | 6091656544647     |               |                   |                   | POST_ENGAGEMENT    | 0.16 | 1                     | 1                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 68                         | 1                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | RESIZE_rexxar1200x628                                      | 23842735695970684 | iOS | US | iPhone | 1% LAL                                 | 23842735694610684 | FB | HS | Display | Evergreen | Mobile | NA | iOS          | 23842735694550684 | acquisition   |                   |                   | APP_INSTALLS       | 0.08 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 6                          | 6                           | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | RESIZE_jaina1200x628                                       | 23842735695060684 | iOS | US | iPhone | 1% LAL                                 | 23842735694610684 | FB | HS | Display | Evergreen | Mobile | NA | iOS          | 23842735694550684 | acquisition   |                   |                   | APP_INSTALLS       | 0.45 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 29                         | 29                          | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
| 2018-05-06 | 0    | America/Los_Angeles | hour        | USD           | HS_SEPT_Resizing1200x628_Spellbender                       | 23842735735450684 | iOS | US | Android Phone | 1% LAL                          | 23842735698820684 | FB | HS | Display | Evergreen | Mobile | NA | Android      | 23842735698750684 | acquisition   |                   |                   | APP_INSTALLS       | 2.10 | 0                     | 0                      | 0                    | 0.00                               | 0.00                                       | 0.00                                         | 0.00                                        | 0.00                  | 0.00                          | 0.00                            | 0.00                           | 0.00                     | 0.00                             | 0.00                               | 0.00                              | 0.00                         | 0.00                                 | 0.00                                   | 0.00                                  | 0                             | 89                         | 89                          | 0                 | 0                          | 0                         | 0                           | 0                          | 0.00             | 0.00                      | 0.00                     | 0.00                       | 0.00                      | USD              |
