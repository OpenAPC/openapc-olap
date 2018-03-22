#!/usr/bin/env python

import datetime
from HTMLParser import HTMLParser
import json
import os
import re
import sys
import urllib2

from util import UnicodeReader, colorise

JOURNAL_ID_RE = re.compile('<a href="/journal/(?P<journal_id>\d+)" title=".*?">', re.IGNORECASE)
SEARCH_RESULTS_COUNT_RE = re.compile('<h1 class="number-of-search-results-and-search-terms">\s*<strong>(?P<count>[\d,]+)</strong>', re.IGNORECASE)
SEARCH_RESULTS_TITLE_RE = re.compile('<p class="message">You are now only searching within the Journal</p>\s*<p class="title">\s*<a href="/journal/\d+">(?P<title>.*?)</a>', re.IGNORECASE | re.UNICODE)

SPRINGER_OA_SEARCH = "https://link.springer.com/search?facet-journal-id={}&package=openaccessarticles&search-within=Journal&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"
SPRINGER_FULL_SEARCH = "https://link.springer.com/search?facet-journal-id={}&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"
SPRINGER_GET_CSV = "https://link.springer.com/search/csv?date-facet-mode=between&search-within=Journal&package=openaccessarticles&facet-journal-id={}&facet-end-year={}&query=&facet-start-year=2015"

COVERAGE_CACHE = {}
PERSISTENT_ARTICLE_CACHE = {} # Persistent cache, loaded from ARTICLE_CACHE_FILE on startup
TEMP_ARTICLE_CACHE = {} # temp cache, results will be included into the persistent cache on shutdown

COVERAGE_CACHE_FILE = "coverage_stats.json"
ARTICLE_CACHE_FILE = "article_pubdates.json"

ARTICLE_CSV_DIR = "coverage_article_files"

ERROR_MSGS = []
             
        
def _query_crossref(issn, year, pause = 1):
    time.sleep(pause) # reduce API load
    start_time = time.time()
    api_url = "http://api.crossref.org/works?filter="
    facet = "&facet=license:*"
    filters = [
        "issn:" + issn,
        "from-pub-date:" + year + "-01-01",
        "until-pub-date:" + year + "-12-31",
        "type:journal-article"
    ]
    headers = {"User-Agent": "OpenAPC treemaps updater (https://treemaps.intact-project.org/); mailto:openapc@uni-bielefeld.de"}
    url = api_url + ",".join(filters) + facet
    print url
    req = urllib2.Request(url, None, headers)
    try:
        response = urllib2.urlopen(req)
        content = json.loads(response.read())
        end_time = time.time()
        print "crossref data received, took {}s (+ {}s pause)".format(round(end_time - start_time, 2), pause)
        return content
    except urllib2.HTTPError as httpe:
        _print("r", "HTTPError while querying crossref: " + httpe.reason)
        _shutdown()
    except urllib2.URLError as urle:
        _print("r", "URLError while querying crossref: " + urle.reason)
        _shutdown()

def _analyse_crossref_data(content):
    try:
        result = {}
        result["num_journal_total_articles"] = content["message"]["total-results"]
        result["num_journal_oa_articles"] = 0
        licenses = content["message"]["facets"]["license"]["values"]
        for lic, count in licenses.iteritems():
            if lic not in CLASSIFICATOR_CACHE:
                msg = ('Encountered unknown license url "{}". Please decide if it denotes ' +
                       '(c)losed access or (o)pen access:')
                msg = msg.format(lic)
                answer = raw_input(msg)
                while answer not in ["c", "o"]:
                    answer = raw_input("Please type 'o' for open access or 'c' for closed access:")
                CLASSIFICATOR_CACHE[lic] = "open" if answer == "o" else "closed"
            if CLASSIFICATOR_CACHE[lic] == "open":
                result["num_journal_oa_articles"] += count
        return result
    except KeyError as ke:
        print "KeyError while accessing crossref JSON structure: " + ke.message
        _shutdown()
         
def _shutdown():
    print "Updating cache files.."
    with open(COVERAGE_CACHE_FILE, "w") as f:
        f.write(json.dumps(COVERAGE_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    with open(ARTICLE_CACHE_FILE, "w") as f:
        f.write(json.dumps(PERSISTENT_ARTICLE_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    print "Done."
    num_articles = 0
    for issn, dois in PERSISTENT_ARTICLE_CACHE.iteritems():
        num_articles += len(dois)
    print "The article cache now contains publication dates for {} DOIs".format(num_articles)
    if ERROR_MSGS:
        print colorise("There were errors during the lookup process:", "yellow")
        for msg in ERROR_MSGS:
            print msg
    sys.exit()
    
def update_coverage_stats(offsetting_file, max_lookups):
    global COVERAGE_CACHE, PERSISTENT_ARTICLE_CACHE
    if os.path.isfile(COVERAGE_CACHE_FILE):
        with open(COVERAGE_CACHE_FILE, "r") as f:
            try:
               COVERAGE_CACHE  = json.loads(f.read())
               print "coverage cache file sucessfully loaded."
            except ValueError:
                print "Could not decode a cache structure from " + COVERAGE_CACHE_FILE + ", starting with an empty coverage cache."
    else:
        print "No cache file (" + COVERAGE_CACHE_FILE + ") found, starting with an empty coverage cache."
    if os.path.isfile(ARTICLE_CACHE_FILE):
        with open(ARTICLE_CACHE_FILE, "r") as f:
            try:
               PERSISTENT_ARTICLE_CACHE  = json.loads(f.read())
               print "Article cache file sucessfully loaded."
            except ValueError:
                print "Could not decode a cache structure from " + ARTICLE_CACHE_FILE + ", starting with an empty classificator cache."
    else:
        print "No cache file (" + ARTICLE_CACHE_FILE + ") found, starting with an empty classificator cache."
    
    if not os.path.isdir(ARTICLE_CSV_DIR):
        raise IOError("Article cache directory " + ARTICLE_CSV_DIR + " not found!")
    
    reader = UnicodeReader(open(offsetting_file, "r"))
    num_lookups = 0
    for line in reader:
        lookup_performed = False
        publisher = line["publisher"]
        if publisher != "Springer Nature":
            continue
        issn = line["issn"]
        period = line["period"]
        title = line["journal_full_title"]
        doi = line["doi"]
        journal_id = None
        # Retreive publication dates for articles from CSV summaries on SpringerLink.
        # Employ a multi-level cache structure to minimize IO:
        #  - try to look up the doi in the persistent journal article cache
        #  - if the journal is not present, repopulate local cache segment from a CSV file in article cache dir
        #  - if no CSV for the journal could be found, fetch it from SpringerLink
        try:
            _ = PERSISTENT_ARTICLE_CACHE[issn][doi]
            print u"Journal {} ('{}'): DOI {} already cached.".format(issn, title, doi)
        except KeyError:
            if issn not in TEMP_ARTICLE_CACHE:
                msg = u"Journal {} ('{}'): Not found in temp cache, repopulating..."
                print msg.format(issn, title)
                journal_id = _get_springer_journal_id_from_doi(doi)
                TEMP_ARTICLE_CACHE[issn] = _get_journal_cache_from_csv(issn, journal_id, refetch=False)
            if doi not in TEMP_ARTICLE_CACHE[issn]:
                msg = u"Journal {} ('{}'): DOI {} not found in cache, re-fetching csv file..."
                print msg.format(issn, title, doi)
                TEMP_ARTICLE_CACHE[issn] = _get_journal_cache_from_csv(issn, journal_id, refetch=True)
                if doi not in TEMP_ARTICLE_CACHE[issn]:
                    msg = u"Journal {} ('{}'): DOI {} NOT FOUND in SpringerLink data!"
                    msg = colorise(msg.format(title, issn, doi), "red")
                    print msg
                    ERROR_MSGS.append(msg)
                    continue
            lookup_performed = True
            if issn not in PERSISTENT_ARTICLE_CACHE:
                PERSISTENT_ARTICLE_CACHE[issn] = {}
            PERSISTENT_ARTICLE_CACHE[issn][doi] = TEMP_ARTICLE_CACHE[issn][doi]
            pub_year = PERSISTENT_ARTICLE_CACHE[issn][doi]
            compare_msg = u"DOI {} found in Springer data, Pub year is {} ".format(doi, pub_year)
            if pub_year == period:
                compare_msg += colorise("(same as offsetting period)", "green")
            else:
                compare_msg += colorise("(DIFFERENT from offsetting period, which is {})".format(period), "yellow")
            msg = u"Journal {} ('{}'): ".format(issn, title)
            print msg.ljust(80) + compare_msg
        # Retreive journal total and OA statistics for every covered publication year.
        pub_year = PERSISTENT_ARTICLE_CACHE[issn][doi]
        if issn not in COVERAGE_CACHE:
            COVERAGE_CACHE[issn] = {}
        if pub_year not in COVERAGE_CACHE[issn]:
            COVERAGE_CACHE[issn][pub_year] = {}
        # If coverage stats are missing, we have to scrap them from the SpringerLink search site HTML
        try:
            _ = COVERAGE_CACHE[issn][pub_year]["num_journal_total_articles"]
        except KeyError:
            msg = u'No cached entry found for total article numbers in journal "{}" ({}) in the {} publication period, querying SpringerLink...'
            print msg.format(title, issn, pub_year)
            if issn not in COVERAGE_CACHE:
                COVERAGE_CACHE[issn] = {}
            if journal_id is None:
                journal_id = _get_springer_journal_id_from_doi(doi)
            total = _get_springer_journal_stats(journal_id, pub_year, oa=False)
            COVERAGE_CACHE[issn][pub_year]["num_journal_total_articles"] = total["count"]
            lookup_performed = True
        try:
            _ = COVERAGE_CACHE[issn][pub_year]["num_journal_oa_articles"]
        except KeyError:
            msg = u'No cached entry found for OA article numbers in journal "{}" ({}) in the {} publication period, querying SpringerLink...'
            print msg.format(title, issn, pub_year)
            if issn not in COVERAGE_CACHE:
                COVERAGE_CACHE[issn] = {}
            if journal_id is None:
                journal_id = _get_springer_journal_id_from_doi(doi)
            oa = _get_springer_journal_stats(journal_id, pub_year, oa=True)
            COVERAGE_CACHE[issn][pub_year]["num_journal_oa_articles"] = oa["count"]
            lookup_performed = True
        if lookup_performed:
            num_lookups += 1
        if max_lookups is not None and num_lookups >= max_lookups:
            print u"maximum number of lookups performed."
            _shutdown()
    _shutdown()
    
def _get_journal_cache_from_csv(issn, journal_id, refetch):
    path = os.path.join(ARTICLE_CSV_DIR, issn + ".csv")
    if not os.path.isfile(path) or refetch:
        _fetch_springer_journal_csv(path, journal_id)
        msg = u"Journal {}: Fetching article CSV table from SpringerLink..."
        print msg.format(issn)
    with open(path) as p:
        reader = UnicodeReader(p)
        cache = {}
        for line in reader:
            doi = line["Item DOI"]
            year = line["Publication Year"]
            cache[doi] = year
        return cache
        
def _fetch_springer_journal_csv(path, journal_id):
    # WARNING: SpringerLink caps CSV size at 1000 lines. This will become a problem
    # when a single journal reaches a total number of more than 1000 OA articles.
    year = datetime.datetime.now().year
    url = SPRINGER_GET_CSV.format(journal_id, year)
    handle = urllib2.urlopen(url)
    content = handle.read()
    with open(path, "wb") as f:
        f.write(content)
    
def _get_springer_journal_id_from_doi(doi):
    if doi.startswith(("10.1007/s", "10.3758/s", "10.1245/s", "10.1617/s", "10.1186/s")):
        return doi[9:14].lstrip("0")
    elif doi.startswith("10.1140"):
    # In case of the "European Physical journal" family, the journal id cannot be extracted directly from the DOI.
        print "No direct journal id extraction possible for doi " + doi + ", analysing landing page..." 
        req = urllib2.Request("https://doi.org/" + doi, None)
        response = urllib2.urlopen(req)
        content = response.read()
        match = JOURNAL_ID_RE.search(content)
        if match:
            journal_id = match.groupdict()["journal_id"]
            print "journal id found: " + journal_id
            return journal_id
        else:
            raise ValueError("Regex could not detect a journal id for doi " + doi)
    else:
        raise ValueError(doi + " does not seem to be a Springer DOI (prefix not in list)!") 
    
def _get_springer_journal_stats(journal_id, period, oa=False, pause=0.5):
    if not journal_id.isdigit():
        raise ValueError("Invalid journal id " + journal_id + " (not a number)")
    url = SPRINGER_FULL_SEARCH.format(journal_id, period, period)
    if oa:
        url = SPRINGER_OA_SEARCH.format(journal_id, period, period)
    print url
    req = urllib2.Request(url, None)
    response = urllib2.urlopen(req)
    content = response.read()
    results = {}
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
        title = unicode(title, "utf-8")
        htmlparser = HTMLParser()
        results['title'] = htmlparser.unescape(title)
    else:
        raise ValueError("Regex could not detect a journal title at " + url)
    return results
