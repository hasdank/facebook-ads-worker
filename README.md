# facebook_ads_worker

***Work In Progress***

Facebook Ads Worker using Facebook Marketing and Insights APIs

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


| field                                 |
|---------------------------------------|
| date                                  |
| hour                                  |
| timezone                              |
| granularity                           |
| campaign_type                         |
| campaign_id                           |
| campaign_name                         |
| adset_id                              |
| adset_name                            |
| ad_id                                 |
| ad_name                               |
| adgroup_name                          |
| received_clicks_gross                 |
| received_clicks_unique                |
| received_conversions                  |
| received_events_other                 |
| received_events_other_1d_view         |
| received_events_other_28d_click       |
| received_events_other_7d_click        |
| received_impressions_complete         |
| received_impressions_gross            |
| received_impressions_unique           |
| received_installs                     |
| received_installs_1d_view             |
| received_installs_28d_click           |
| received_installs_7d_click            |
| received_revenue                      |
| received_revenue_1d_view              |
| received_revenue_28d_click            |
| received_revenue_7d_click             |


# Results Parsing

## Reports

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

## Python Script `results_parser.py`

### Install Requirements

```bash
$ brew install cairo pango gdk-pixbuf libffi
$ python3 -m install weasyprint
$ python3 -m install pandas
$ python3 -m install jinja2
$ python3 -m install numpy
```

### Run

```bash
$ cd cost/facebook_ads
$ make results
======================================================
results-pandas tmp/facebook_ads.json.gz
======================================================
-rw-r--r--  1 jefft  staff  14193 May 09 17:12 tmp/facebook_ads.json.gz
'Results: tmp/facebook_ads.json.gz'
'Results CSV: tmp/facebook_ads.json.results.csv'
'Summary CSV: tmp/facebook_ads.json.summary.csv'
'TEX: tmp/facebook_ads.json.summary.tex'
'PDF: tmp/facebook_ads.json.summary.pdf'
-rw-r--r--  1 jefft  staff  208093 May 09 13:52 tmp/facebook_ads.json.results.csv
-rw-r--r--  1 jefft  staff   12163 May 09 13:52 tmp/facebook_ads.json.summary.csv
-rw-r--r--  1 jefft  staff  34732 May 09 13:52 tmp/facebook_ads.json.summary.tex
-rw-r--r--  1 jefft  staff  106162 May 09 13:52 tmp/facebook_ads.json.summary.pdf
```

## Python Script `results_parser.py`

### Run

```bash
$ cd cost/facebook_ads
$ make local-run-no-install
```

```json
{"asctime": "2018-05-09 17:12:21 -0800", "levelname": "INFO", "name": "tune_mv_integration", "version": "1.23.7", "message": "Printed: File: Finished", "upload_file_exists": true, "upload_file_path": "/Users/jefft/github/TuneLab/mv-integrations/cost/facebook_ads/tmp/facebook_ads.json.gz", "upload_file_size": "13.86 KB", "upload_row_count": 657, "upload_row_count_printed": 657}
```

```bash
$ ls -al tmp
total 32
drwxr-xr-x   3 jefft  staff    102 May 09 17:12 .
drwxr-xr-x  23 jefft  staff    782 May 09 17:12 ..
-rw-r--r--   1 jefft  staff  14193 May 09 17:12 facebook_ads.json.gz

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

#### `tmp/[COLLECTOR].results.csv`

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
client_id,partner_id,vendor_id,vendor_account_id,date,hour,timezone,granularity,platform_ref,platform_type,site_ref_id,site_ref_type,campaign_type,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,my_campaign_name,agency_id,received_impressions_gross,received_impressions_unique,received_impressions_complete,received_clicks_gross,received_clicks_unique,received_conversions,received_installs,received_installs_7d_click,received_installs_28d_click,received_installs_1d_view,received_revenue,received_revenue_7d_click,received_revenue_28d_click,received_revenue_1d_view,cost,cost_currency,received_events_other,received_events_other_7d_click,received_events_other_28d_click,received_events_other_1d_view
1,7,1650,1498663353579121,"2018-01-06",14,"America/Los_Angeles","hour","com.hellochatty","android",2385,"tmc","acquisition",23842716148040455,"FAR Hello Chatty App installs - Brent Testing",23842716148100455,"US - 18+",23842716148220455,"Default Name - App installs - Image 1","",3,256,0,0,8,0,0,3,3,3,0,0.0,0.0,0.0,0.0,2.19,"USD",122,76,122,
1,7,1650,1498663353579121,"2018-01-06",14,"America/Los_Angeles","hour","com.hellochatty","android",2385,"tmc","acquisition",23842716148040455,"FAR Hello Chatty App installs - Brent Testing",23842716148100455,"US - 18+",23842716148380455,"Default Name - App installs - Image 2","",3,128,0,0,1,0,0,0,0,0,0,0.0,0.0,0.0,0.0,0.79,"USD",,,,
1,7,1650,1498663353579121,"2018-01-06",15,"America/Los_Angeles","hour","com.hellochatty","android",2385,"tmc","acquisition",23842716148040455,"FAR Hello Chatty App installs - Brent Testing",23842716148100455,"US - 18+",23842716148220455,"Default Name - App installs - Image 1","",3,248,0,0,4,0,0,3,3,3,0,0.0,0.0,0.0,0.0,2.78,"USD",31,31,31,
1,7,1650,1498663353579121,"2018-01-06",15,"America/Los_Angeles","hour","com.hellochatty","android",2385,"tmc","acquisition",23842716148040455,"FAR Hello Chatty App installs - Brent Testing",23842716148100455,"US - 18+",23842716148380455,"Default Name - App installs - Image 2","",3,60,0,0,0,0,0,0,0,0,0,0.0,0.0,0.0,0.0,0.24,"USD",,,,
```

| client_id | partner_id | vendor_id | vendor_account_id | date         | hour | timezone              | granularity | platform_ref      | platform_type | site_ref_id | site_ref_type | campaign_type | campaign_id  | campaign_name                               | adset_id   | adset_name | ad_id        | ad_name                                | my_campaign_name | agency_id | received_impressions_gross | received_impressions_unique | received_impressions_complete | received_clicks_gross | received_clicks_unique | received_conversions | received_installs | received_installs_7d_click | received_installs_28d_click | received_installs_1d_view | received_revenue | received_revenue_7d_click | received_revenue_28d_click | received_revenue_1d_view | cost  | cost_currency | received_events_other | received_events_other_7d_click | received_events_other_28d_click | received_events_other_1d_view |
|-----------|------------|-----------|-------------------|--------------|------|-----------------------|-------------|-------------------|---------------|-------------|---------------|-------------------|-------------------|-------------------------------------------------|-------------------|------------------|-------------------|--------------------------------------------|------------------|-----------|----------------------------|-----------------------------|-------------------------------|-----------------------|------------------------|----------------------|-------------------|----------------------------|-----------------------------|---------------------------|------------------|---------------------------|----------------------------|--------------------------|-------|---------------|-----------------------|--------------------------------|---------------------------------|-------------------------------|
| 1         | 7          | 1650      | 1498663353579121  | "2018-01-06" | 14   | "America/Los_Angeles" | "hour"      | "com.hellochatty" | "android"     | 2385        | "tmc"         | "acquisition"     | 23842716148040455 | "FAR Hello Chatty App installs - Brent Testing" | 23842716148100455 | "US - 18+"       | 23842716148220455 | "Default Name - App installs - Image 1","" | 3                | 256       | 0                          | 0                           | 8                             | 0                     | 0                      | 3                    | 3                 | 3                          | 0                           | 0.0                       | 0.0              | 0.0                       | 0.0                        | 2.19                     | "USD" | 122           | 76                    | 122                            |                                 |                               |
| 1         | 7          | 1650      | 1498663353579121  | "2018-01-06" | 14   | "America/Los_Angeles" | "hour"      | "com.hellochatty" | "android"     | 2385        | "tmc"         | "acquisition"     | 23842716148040455 | "FAR Hello Chatty App installs - Brent Testing" | 23842716148100455 | "US - 18+"       | 23842716148380455 | "Default Name - App installs - Image 2","" | 3                | 128       | 0                          | 0                           | 1                             | 0                     | 0                      | 0                    | 0                 | 0                          | 0                           | 0.0                       | 0.0              | 0.0                       | 0.0                        | 0.79                     | "USD" |               |                       |                                |                                 |                               |
| 1         | 7          | 1650      | 1498663353579121  | "2018-01-06" | 15   | "America/Los_Angeles" | "hour"      | "com.hellochatty" | "android"     | 2385        | "tmc"         | "acquisition"     | 23842716148040455 | "FAR Hello Chatty App installs - Brent Testing" | 23842716148100455 | "US - 18+"       | 23842716148220455 | "Default Name - App installs - Image 1","" | 3                | 248       | 0                          | 0                           | 4                             | 0                     | 0                      | 3                    | 3                 | 3                          | 0                           | 0.0                       | 0.0              | 0.0                       | 0.0                        | 2.78                     | "USD" | 31            | 31                    | 31                             |                                 |                               |
| 1         | 7          | 1650      | 1498663353579121  | "2018-01-06" | 15   | "America/Los_Angeles" | "hour"      | "com.hellochatty" | "android"     | 2385        | "tmc"         | "acquisition"     | 23842716148040455 | "FAR Hello Chatty App installs - Brent Testing" | 23842716148100455 | "US - 18+"       | 23842716148380455 | "Default Name - App installs - Image 2","" | 3                | 60        | 0                          | 0                           | 0                             | 0                     | 0                      | 0                    | 0                 | 0                          | 0                           | 0.0                       | 0.0              | 0.0                       | 0.0                        | 0.24                     | "USD" |               |                       |                                |                                 |                               |


#### `tmp/[COLLECTOR].summary.csv`

Contents of this file is a sum of JSON responses to CSV.

Grouped by
* date
* ad_id

All of the following columns are the sum
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
client_id,vendor_id,partner_id,agency_id,vendor_account_id,date,timezone,site_ref_id,site_ref_type,campaign_type,campaign_name,campaign_id,ad_name,ad_id,adset_id,adset_name,platform_ref,platform_type,cost_currency,my_campaign_name,cost,received_clicks_gross,received_clicks_unique,received_conversions,received_events_other,received_events_other_1d_view,received_events_other_28d_click,received_events_other_7d_click,received_impressions_complete,received_impressions_gross,received_impressions_unique,received_installs,received_installs_1d_view,received_installs_28d_click,received_installs_7d_click,received_revenue,received_revenue_1d_view,received_revenue_28d_click,received_revenue_7d_click
1,1650,7,3,1498663353579121,"2018-01-06","America/Los_Angeles",2385,"tmc","acquisition","FAR Hello Chatty App installs - Brent Testing","23842716148040455","Default Name - App installs - Image 1","23842716148220455","23842716148100455","US - 18+","com.hellochatty","android","USD","",18.119999999999997,49,0,0,514,,514,422,0,1924,0,16,0,16,16,0.0,0.0,0.0,0.0
1,1650,7,3,1498663353579121,"2018-01-06","America/Los_Angeles",2385,"tmc","acquisition","FAR Hello Chatty App installs - Brent Testing","23842716148040455","Default Name - App installs - Image 2","23842716148380455","23842716148100455","US - 18+","com.hellochatty","android","USD","",3.47,13,0,0,32,,32,32,0,441,0,6,0,6,6,0.0,0.0,0.0,0.0
1,1650,7,3,1498663353579121,"2018-01-07","America/Los_Angeles",2385,"tmc","acquisition","FAR Hello Chatty App installs - Brent Testing","23842716148040455","Default Name - App installs - Image 1","23842716148220455","23842716148100455","US - 18+","com.hellochatty","android","USD","",20.63,36,0,0,248,,248,248,0,1601,0,11,0,11,11,0.0,0.0,0.0,0.0
1,1650,7,3,1498663353579121,"2018-01-07","America/Los_Angeles",2385,"tmc","acquisition","FAR Hello Chatty App installs - Brent Testing","23842716148040455","Default Name - App installs - Image 2","23842716148380455","23842716148100455","US - 18+","com.hellochatty","android","USD","",30.000000000000004,95,0,0,1627,,1627,1312,0,3155,0,50,1,49,49,0.0,0.0,0.0,0.0
1,1650,7,3,1498663353579121,"2018-01-08","America/Los_Angeles",2385,"tmc","acquisition","FAR Hello Chatty App installs - Brent Testing","23842716148040455","Default Name - App installs - Image 1","23842716148220455","23842716148100455","US - 18+","com.hellochatty","android","USD","",1.06,2,0,0,,,,,0,77,0,0,0,0,0,0.0,0.0,0.0,0.0
```

| client_id | vendor_id | partner_id | agency_id | vendor_account_id | date         | timezone              | site_ref_id | site_ref_type | campaign_type | campaign_name                               | campaign_id    | ad_name                             | ad_id          | adset_id     | adset_name | platform_ref      | platform_type | cost_currency | my_campaign_name   | cost | received_clicks_gross | received_clicks_unique | received_conversions | received_events_other | received_events_other_1d_view | received_events_other_28d_click | received_events_other_7d_click | received_impressions_complete | received_impressions_gross | received_impressions_unique | received_installs | received_installs_1d_view | received_installs_28d_click | received_installs_7d_click | received_revenue | received_revenue_1d_view | received_revenue_28d_click | received_revenue_7d_click |
|-----------|-----------|------------|-----------|-------------------|--------------|-----------------------|-------------|---------------|-------------------|-------------------------------------------------|---------------------|-----------------------------------------|---------------------|---------------------|------------------|-------------------|---------------|---------------|--------------------|------|-----------------------|------------------------|----------------------|-----------------------|-------------------------------|---------------------------------|--------------------------------|-------------------------------|----------------------------|-----------------------------|-------------------|---------------------------|-----------------------------|----------------------------|------------------|--------------------------|----------------------------|---------------------------|
| 1         | 1650      | 7          | 3         | 1498663353579121  | "2018-01-06" | "America/Los_Angeles" | 2385        | "tmc"         | "acquisition"     | "FAR Hello Chatty App installs - Brent Testing" | "23842716148040455" | "Default Name - App installs - Image 1" | "23842716148220455" | "23842716148100455" | "US - 18+"       | "com.hellochatty" | "android"     | "USD",""      | 18.119999999999997 | 49   | 0                     | 0                      | 514                  |                       | 514                           | 422                             | 0                              | 1924                          | 0                          | 16                          | 0                 | 16                        | 16                          | 0.0                        | 0.0              | 0.0                      | 0.0                        |                           |
| 1         | 1650      | 7          | 3         | 1498663353579121  | "2018-01-06" | "America/Los_Angeles" | 2385        | "tmc"         | "acquisition"     | "FAR Hello Chatty App installs - Brent Testing" | "23842716148040455" | "Default Name - App installs - Image 2" | "23842716148380455" | "23842716148100455" | "US - 18+"       | "com.hellochatty" | "android"     | "USD",""      | 3.47               | 13   | 0                     | 0                      | 32                   |                       | 32                            | 32                              | 0                              | 441                           | 0                          | 6                           | 0                 | 6                         | 6                           | 0.0                        | 0.0              | 0.0                      | 0.0                        |                           |
| 1         | 1650      | 7          | 3         | 1498663353579121  | "2018-01-07" | "America/Los_Angeles" | 2385        | "tmc"         | "acquisition"     | "FAR Hello Chatty App installs - Brent Testing" | "23842716148040455" | "Default Name - App installs - Image 1" | "23842716148220455" | "23842716148100455" | "US - 18+"       | "com.hellochatty" | "android"     | "USD",""      | 20.63              | 36   | 0                     | 0                      | 248                  |                       | 248                           | 248                             | 0                              | 1601                          | 0                          | 11                          | 0                 | 11                        | 11                          | 0.0                        | 0.0              | 0.0                      | 0.0                        |                           |
| 1         | 1650      | 7          | 3         | 1498663353579121  | "2018-01-07" | "America/Los_Angeles" | 2385        | "tmc"         | "acquisition"     | "FAR Hello Chatty App installs - Brent Testing" | "23842716148040455" | "Default Name - App installs - Image 2" | "23842716148380455" | "23842716148100455" | "US - 18+"       | "com.hellochatty" | "android"     | "USD",""      | 30.000000000000004 | 95   | 0                     | 0                      | 1627                 |                       | 1627                          | 1312                            | 0                              | 3155                          | 0                          | 50                          | 1                 | 49                        | 49                          | 0.0                        | 0.0              | 0.0                      | 0.0                        |                           |
| 1         | 1650      | 7          | 3         | 1498663353579121  | "2018-01-08" | "America/Los_Angeles" | 2385        | "tmc"         | "acquisition"     | "FAR Hello Chatty App installs - Brent Testing" | "23842716148040455" | "Default Name - App installs - Image 1" | "23842716148220455" | "23842716148100455" | "US - 18+"       | "com.hellochatty" | "android"     | "USD",""      | 1.06               | 2    | 0                     | 0                      |                      |                       |                               |                                 | 0                              | 77                            | 0                          | 0                           | 0                 | 0                         | 0                           | 0.0                        | 0.0              | 0.0                      | 0.0                        |                           |
