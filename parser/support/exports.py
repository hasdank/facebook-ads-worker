#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Results Parser: Export

import os
from pprintpp import pprint
import jinja2

def exportFilePrep(resultsFilePath, report, type):
    head, tail = os.path.split(resultsFilePath)
    filename = tail.split(".")[0]
    filepath = f"{head}/reports"

    resultsFile = f"{filepath}/{filename}.{report}.{type}"
    os.remove(resultsFile) if os.path.exists(resultsFile) else None
    return resultsFile


def exportResultsCSV(resultsFilePath, report, df):
    resultsFile = exportFilePrep(resultsFilePath, report, "csv")
    df.to_csv(resultsFile, sep=',', encoding='utf-8', index=False, float_format='%.2f')

    if os.path.exists(resultsFile):
        pprint(f"Results CSV: {resultsFile}")

    return df, resultsFile


def exportResultsTEX(resultsFilePath, report, df):
    resultsFile = exportFilePrep(resultsFilePath, report, "tex")
    with open(resultsFile, 'w') as tf:
        tf.write(df.to_latex())

    if os.path.exists(resultsFile):
        pprint(f"TEX: {resultsFile}")

    return df, resultsFile


def exportResultsHTML(resultsFilePath, report, df):
    resultsFile = exportFilePrep(resultsFilePath, report, "html")

    html_template = """
    <!DOCTYPE html>
    <html lang=\"en\">
    <head><meta charset=\"UTF-8\">
    <style>
        .dataframe { font-family: "Trebuchet MS", Arial, Helvetica, sans-serif; border-collapse: collapse; width: 100%; }
        .dataframe table {
            table-layout: fixed;
            border-collapse: collapse;
            overflow-y: auto;
            height: 200px;
            border-spacing: 0;
            width:100%;
        }
        .dataframe td, .dataframe th {
            border:1px solid #ddd;
            padding:8px;
            font-size:11px;
            white-space:nowrap;
        }
        .dataframe th { padding-top: 12px; padding-bottom: 12px; text-align: left; background-color: #4CAF50; color: white; }
        .dataframe tr:nth-child(even){background-color: #f2f2f2;}
    </style>
    </head>
    <body>{{ results_table }}</body>
    </html>
    """

    results_template = jinja2.Template(html_template)
    results_template_vars = {"results_table": df.to_html(index=False, classes='dataframe')}

    rendered_html = results_template.render(results_template_vars)

    rendered_html = rendered_html.replace("&lt;", "<")
    rendered_html = rendered_html.replace("&gt;", ">")
    rendered_html = rendered_html.replace('\n', '')

    with open(resultsFile, 'w') as html:
        html.write(rendered_html)

    if os.path.exists(resultsFile):
        pprint(f"Results HTML: {resultsFile}")

    return rendered_html