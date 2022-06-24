#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import csv
import datetime
from html.parser import HTMLParser
import json
import os
import re
import sys
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from util import colorise

JOURNAL_ID_RE = re.compile(r'<a href="/journal/(?P<journal_id>\d+)" title=".*?">', re.IGNORECASE)
SEARCH_RESULTS_COUNT_RE = re.compile(r'<h1 id="number-of-search-results-and-search-terms">\s*<strong>(?P<count>[\d,]+)</strong>', re.IGNORECASE)
SEARCH_RESULTS_TITLE_RE = re.compile(r'<p class="message">You are now only searching within the Journal</p>\s*<p class="title">\s*<a href="/journal/\d+">(?P<title>.*?)</a>', re.IGNORECASE | re.UNICODE)

ISSN_RE = re.compile(r"^(?P<first_part>\d{4})-?(?P<second_part>\d{3})(?P<check_digit>[\dxX])$")

SPRINGER_OA_SEARCH = "https://link.springer.com/search?facet-journal-id={}&package=openaccessarticles&search-within=Journal&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"
SPRINGER_FULL_SEARCH = "https://link.springer.com/search?facet-journal-id={}&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"
SPRINGER_GET_CSV = "https://link.springer.com/search/csv?date-facet-mode=between&search-within=Journal&facet-journal-id={}&facet-start-year={}&facet-end-year={}&query="

COVERAGE_CACHE = {}
PERSISTENT_PUBDATES_CACHE = {} # Persistent cache, loaded from PUBDATES_CACHE_FILE on startup

TEMP_JOURNAL_CACHE = {} # keeps cached journal statistics imported from CSV files. Intended to reduce I/O workload when multiple articles from the same journal have to be looked up. 

JOURNAL_ID_CACHE = None # keeps journal IDs cached which had to be retreived from SpringerLink to avoid multiple lookups.

COVERAGE_CACHE_FILE = "coverage_stats.json"
PUBDATES_CACHE_FILE = "article_pubdates.json"
JOURNAL_ID_CACHE_FILE = "journal_ids.json"

JOURNAL_CSV_DIR = "coverage_article_files"

# A directory containing the official annual journal lists published by Springer.
# Can be obtained from https://www.springernature.com/gp/librarians/licensing/journals-price-list
# Excel files will need some preprocessing:
# - Remove unused rows above table
# - Export as CSV
# - Rename according to year (e.g. "2015.csv")
SPRINGER_JOURNAL_LISTS_DIR = "springer_journal_lists"

ERROR_MSGS = []
LOOKUPS_PERFORMED = None


def _shutdown():
    """
    Write cache content back to disk before terminating and display collected error messages.
    """
    print("Updating cache files..")
    with open(COVERAGE_CACHE_FILE, "w") as f:
        f.write(json.dumps(COVERAGE_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    with open(PUBDATES_CACHE_FILE, "w") as f:
        f.write(json.dumps(PERSISTENT_PUBDATES_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    with open(JOURNAL_ID_CACHE_FILE, "w") as f:
        f.write(json.dumps(JOURNAL_ID_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    print("Done.")
    num_articles = 0
    for _, dois in PERSISTENT_PUBDATES_CACHE.items():
        num_articles += len(dois)
    print("The article cache now contains publication dates for {} DOIs".format(num_articles))
    if ERROR_MSGS:
        print(colorise("There were errors during the lookup process:", "yellow"))
        for msg in ERROR_MSGS:
            print(msg)
    sys.exit()

def _process_springer_catalogue(max_lookups=None):
    global COVERAGE_CACHE, LOOKUPS_PERFORMED
    current_year = datetime.datetime.now().year
    years = [str(year) for year in range(2015, current_year + 1)]
    for year in years:
        # Perform a simple check before wasting any time on processing
        catalogue_file = os.path.join(SPRINGER_JOURNAL_LISTS_DIR, year + ".csv")
        if not os.path.isfile(catalogue_file):
            raise IOError("Catalogue file " + catalogue_file + " not found!")
    for year in years:
        msg = "Looking up coverage stats for Open Choice journals in " + year
        print(colorise("--- " + msg + " ---", "green"))
        catalogue_file = os.path.join(SPRINGER_JOURNAL_LISTS_DIR, year + ".csv")
        reader = csv.DictReader(open(catalogue_file, "r"))
        for line in reader:
            if max_lookups is not None and LOOKUPS_PERFORMED >= max_lookups:
                return
            title = line["Title"]
            oa_option = line["Open Access Option"]
            if oa_option != "Hybrid (Open Choice)":
                msg = u'Journal "{}" is not an Open Choice journal (oa_option={}), skipping...'
                print(colorise(msg.format(title, oa_option), "yellow"))
                continue
            journal_id = line["product_id"]
            already_cached = True
            try:
                _ = COVERAGE_CACHE[journal_id]['years'][year]["num_journal_total_articles"]
                _ = COVERAGE_CACHE[journal_id]['years'][year]["num_journal_oa_articles"]
            except KeyError:
                try:
                    _update_journal_stats(title, journal_id, year)
                except ValueError as ve:
                    error_msg = ('Journal "{}" ({}): ValueError while obtaining journal ' +
                                 'stats, annual stats not added to cache.')
                    error_msg = colorise(error_msg.format(title, journal_id), "red")
                    print(error_msg)
                    ERROR_MSGS.append(error_msg)
                    continue
                LOOKUPS_PERFORMED += 1
                already_cached = False
            if already_cached:
                msg = 'Stats for journal "{}" in {} already cached.'
                print(colorise(msg.format(title, year), "yellow"))

def _update_journal_stats(title, journal_id, year, verbose=True):
    global COVERAGE_CACHE
    total = _get_springer_journal_stats(journal_id, year, oa=False)
    oa = _get_springer_journal_stats(journal_id, year, oa=True)
    if verbose:
        msg = 'Obtained stats for journal "{}" in {}: {} OA, {} Total'
        print(colorise(msg.format(title, year, oa["count"], total["count"]), "green"))
    if journal_id not in COVERAGE_CACHE:
        COVERAGE_CACHE[journal_id] = {'title': title, 'years': {}}
    if year not in COVERAGE_CACHE[journal_id]['years']:
        COVERAGE_CACHE[journal_id]['years'][year] = {}
    COVERAGE_CACHE[journal_id]['years'][year]["num_journal_total_articles"] = total["count"]
    COVERAGE_CACHE[journal_id]['years'][year]["num_journal_oa_articles"] = oa["count"]

def update_coverage_stats(transformative_agreements_file, max_lookups, refetch=True):
    global COVERAGE_CACHE, JOURNAL_ID_CACHE, PERSISTENT_PUBDATES_CACHE, LOOKUPS_PERFORMED
    LOOKUPS_PERFORMED = 0
    if os.path.isfile(COVERAGE_CACHE_FILE):
        with open(COVERAGE_CACHE_FILE, "r") as f:
            try:
                COVERAGE_CACHE = json.loads(f.read())
                print("coverage cache file sucessfully loaded.")
            except ValueError:
                print("Could not decode a cache structure from " + COVERAGE_CACHE_FILE + ", starting with an empty coverage cache.")
    else:
        print("No cache file (" + COVERAGE_CACHE_FILE + ") found, starting with an empty coverage cache.")
    if os.path.isfile(PUBDATES_CACHE_FILE):
        with open(PUBDATES_CACHE_FILE, "r") as f:
            try:
                PERSISTENT_PUBDATES_CACHE = json.loads(f.read())
                print("Pub dates cache file sucessfully loaded.")
            except ValueError:
                print("Could not decode a cache structure from " + PUBDATES_CACHE_FILE + ", starting with an empty pub date cache.")
    else:
        print("No cache file (" + PUBDATES_CACHE_FILE + ") found, starting with an empty pub date cache.")
        
    if not os.path.isdir(JOURNAL_CSV_DIR):
        raise IOError("Journal CSV directory " + JOURNAL_CSV_DIR + " not found!")

    _process_springer_catalogue(max_lookups)

    reader = csv.DictReader(open(transformative_agreements_file, "r"))
    for line in reader:
        if max_lookups is not None and LOOKUPS_PERFORMED >= max_lookups:
            print("maximum number of lookups performed.")
            _shutdown()
        lookup_performed = False
        found = True
        publisher = line["publisher"]
        if publisher != "Springer Nature":
            continue
        issn = line["issn"]
        period = line["period"]
        title = line["journal_full_title"]
        doi = line["doi"]
        journal_id = _get_springer_journal_id_from_doi(doi, issn)
        # Retreive publication dates for articles from CSV summaries on SpringerLink.
        # Employ a multi-level cache structure to minimize IO:
        #  1. try to look up the doi in the persistent publication dates cache
        #  2. if the journal is not present, repopulate local cache segment from a CSV file in the journal CSV dir
        #  3a. if no CSV for the journal could be found, fetch it from SpringerLink
        #  3b. Alternative to 3: If a CSV was found but it does not contain the DOI, re-fetch it from SpringerLink 
        try:
            _ = PERSISTENT_PUBDATES_CACHE[journal_id][doi]
            print("Journal {} ('{}'): DOI {} already cached.".format(journal_id, title, doi))
        except KeyError:
            if journal_id not in TEMP_JOURNAL_CACHE:
                msg = "Journal {} ('{}'): Not found in temp cache, repopulating..."
                print(msg.format(journal_id, title))
                TEMP_JOURNAL_CACHE[journal_id] = _get_journal_cache_from_csv(journal_id, refetch=False)
            if doi not in TEMP_JOURNAL_CACHE[journal_id]:
                if refetch:
                    msg = u"Journal {} ('{}'): DOI {} not found in cache, re-fetching csv file..."
                    print(msg.format(journal_id, title, doi))
                    TEMP_JOURNAL_CACHE[journal_id] = _get_journal_cache_from_csv(journal_id, refetch=True)
                if doi not in TEMP_JOURNAL_CACHE[journal_id]:
                    msg = u"Journal {} ('{}'): DOI {} NOT FOUND in SpringerLink data!"
                    msg = colorise(msg.format(title, journal_id, doi), "red")
                    print(msg)
                    ERROR_MSGS.append(msg)
                    found = False
            lookup_performed = True
            if journal_id not in PERSISTENT_PUBDATES_CACHE:
                PERSISTENT_PUBDATES_CACHE[journal_id] = {}
            if found:
                PERSISTENT_PUBDATES_CACHE[journal_id][doi] = TEMP_JOURNAL_CACHE[journal_id][doi]
                pub_year = PERSISTENT_PUBDATES_CACHE[journal_id][doi]
                compare_msg = u"DOI {} found in Springer data, Pub year is {} ".format(doi, pub_year)
                if pub_year == period:
                    compare_msg += colorise("(same as transformative_agreements period)", "green")
                else:
                    compare_msg += colorise("(DIFFERENT from transformative_agreements period, which is {})".format(period), "yellow")
                msg = u"Journal {} ('{}'): ".format(journal_id, title)
                print(msg.ljust(80) + compare_msg)
        if found:
            pub_year = PERSISTENT_PUBDATES_CACHE[journal_id][doi]
        else:
            # If a lookup error occured we will retreive coverage stats for the period year instead, since
            # the aggregation process will make use of this value.
            pub_year = period
        # Test if journal stats are present
        try:
            _ = COVERAGE_CACHE[journal_id]['years'][pub_year]["num_journal_total_articles"]
            _ = COVERAGE_CACHE[journal_id]['years'][pub_year]["num_journal_oa_articles"]
        except KeyError:
            try:
                _update_journal_stats(title, journal_id, pub_year)
                lookup_performed = True
                error_msg = ('No stats found for journal "{}" ({}) in {} albeit having ' +
                             'downloaded the full Open Choice catalogue. Stats were ' +
                             'obtained retroactively.')
                error_msg = colorise(error_msg.format(title, journal_id, pub_year), "red")
                print(error_msg)
                ERROR_MSGS.append(error_msg)
            except ValueError as ve:
                error_msg = ('Critical Error while processing DOI {}: No stats found ' +
                             ' for journal "{}" ({}) in {} albeit having downloaded the ' +
                             'full Open Choice catalogue and stats could not be obtained ' +
                             'retroactively (ValueError: {}).')
                error_msg = colorise(error_msg.format(doi, title, journal_id, pub_year, str(ve)), "red")
                print(error_msg)
                ERROR_MSGS.append(error_msg)
                _shutdown()
        if lookup_performed:
            LOOKUPS_PERFORMED += 1
    _shutdown()

def _get_journal_cache_from_csv(journal_id, refetch):
    """
    Get a mapping dict (doi -> pub_year) from a SpringerLink CSV.

    Open a Springerlink search results CSV file and obtain a doi -> pub_year
    mapping from the "Item DOI" and "Publication Year" columns. Download the file
    first if necessary or advised.

    Args:
        journal_id: The SpringerLink internal journal ID. Can be obtained
                    using _get_springer_journal_id_from_doi()
        refetch: Bool. If True, the CSV file will always be re-downloaded, otherwise
                 a local copy will be tried first.

    Returns:
        A dict with a doi -> pub_year mapping.
    """
    path = os.path.join(JOURNAL_CSV_DIR, journal_id + ".csv")
    if not os.path.isfile(path) or refetch:
        _fetch_springer_journal_csv(path, journal_id)
        msg = u"Journal {}: Fetching article CSV table from SpringerLink..."
        print(msg.format(journal_id))
    with open(path) as p:
        reader = csv.DictReader(p)
        cache = {}
        for line in reader:
            doi = line["Item DOI"]
            year = line["Publication Year"]
            cache[doi] = year
        return cache

def _fetch_springer_journal_csv(path, journal_id):
    current_year = datetime.datetime.now().year
    years = range(2015, current_year + 1)
    joint_lines = []
    for year in years:
        url = SPRINGER_GET_CSV.format(journal_id, year, year)
        handle = urlopen(url)
        if year > 2015:
            handle.readline() # read the CSV header only once
        for line in handle:
            line = line.decode("utf-8")
            if not line.endswith("\n"):
                line += "\n"
            joint_lines.append(line)
    with open(path, "w") as f:
        f.write("".join(joint_lines))

def _get_springer_journal_id_from_doi(doi, issn=None):
    global JOURNAL_ID_CACHE
    if JOURNAL_ID_CACHE is None:
        if os.path.isfile(JOURNAL_ID_CACHE_FILE):
            with open(JOURNAL_ID_CACHE_FILE, "r") as f:
                try:
                    JOURNAL_ID_CACHE = json.loads(f.read())
                    print("journal_id cache file sucessfully loaded.")
                    if JOURNAL_ID_CACHE is None:
                        JOURNAL_ID_CACHE = {}
                except ValueError:
                    print("Could not decode a cache structure from " + JOURNAL_ID_CACHE_FILE + ", starting with an empty journal_id cache.")
                    JOURNAL_ID_CACHE = {}
        else:
            print("No cache file (" + JOURNAL_ID_CACHE_FILE + ") found, starting with an empty journal_id cache.")
            JOURNAL_ID_CACHE = {}
    if doi.startswith(("10.1007/s", "10.3758/s", "10.1245/s", "10.1617/s", "10.1186/s", "10.1208/s", "10.1365/s", "10.1038/s", "10.1057/s", "10.2478/s", "10.1557/s")):
        return doi[9:14].lstrip("0")
    elif doi.startswith(("10.14283")): # Irregular prefix, contains only the "Journal of Frailty & Aging"
        return "42415"
    elif doi.startswith(("10.1631")): # Irregular, "Journal of Zhejiang University-SCIENCE A"
        return "11582"
    elif doi.startswith(("10.1140","10.17269")):
    # In case of these journals, the id cannot be extracted directly from the DOI. (EPJ family, Canadian Public Health Association)
        if issn is None or issn not in JOURNAL_ID_CACHE:
            print("No local journal id extraction possible for doi " + doi + ", analysing landing page...")
            req = Request("https://doi.org/" + doi, None)
            response = urlopen(req)
            content = response.read()
            content = content.decode("utf-8")
            match = JOURNAL_ID_RE.search(content)
            if match:
                journal_id = match.groupdict()["journal_id"]
                print("journal id found: " + journal_id)
                if issn:
                    JOURNAL_ID_CACHE[issn] = journal_id
                return journal_id
            else:
                raise ValueError("Regex could not detect a journal id for doi " + doi)
        else:
            return JOURNAL_ID_CACHE[issn]
    else:
        raise ValueError(doi + " does not seem to be a Springer DOI (prefix not in list)!") 

def _get_springer_journal_stats(journal_id, period, oa=False):
    if not journal_id.isdigit():
        raise ValueError("Invalid journal id " + journal_id + " (not a number)")
    url = SPRINGER_FULL_SEARCH.format(journal_id, period, period)
    if oa:
        url = SPRINGER_OA_SEARCH.format(journal_id, period, period)
    print(url)
    try:
        req = Request(url, None)
        response = urlopen(req)
        content = response.read()
        content = content.decode("utf-8")
        results = {}
    except HTTPError as httpe:
        if httpe.code == 503: # retry on timeout
            print(colorise("Timeout (HTTP 503), retrying...", "yellow"))
            return _get_springer_journal_stats(journal_id, period, oa)
        else:
            raise httpe
    count_match = SEARCH_RESULTS_COUNT_RE.search(content)
    if count_match:
        count = count_match.groupdict()['count']
        count = count.replace(",", "")
        results['count'] = int(count)
    else:
        raise ValueError("Regex could not detect a results count at " + url)
    title_match = SEARCH_RESULTS_TITLE_RE.search(content)
    if title_match:
        title = (title_match.groupdict()['title'])
        htmlparser = HTMLParser()
        results['title'] = htmlparser.unescape(title)
    else:
        raise ValueError("Regex could not detect a journal title at " + url)
    return results
