#!/usr/bin/env python

import argparse
import csv
import codecs
import ConfigParser
import json
import os
import re
import sys
import time
import urllib2

import sqlalchemy

# These two classes were adopted from 
# https://docs.python.org/2/library/csv.html#examples
# UnicodeReader was slightly modified to return a DictReader
class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")
        
class UnicodeReader(object):
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.DictReader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return {k: unicode(v, "utf-8") for (k, v) in row.iteritems()}

    def __iter__(self):
        return self
        
ARG_HELP_STRINGS = {
    
    "dir": "A path to a directory where the generated output files should be stored. " +
           "If omitted, output will be written to the current directory.",
    "num_api_lookups": "stop execution after n journal lookups to " +
                        "when performing the coverage_stats job. Useful for " +
                        "reducing API loads and saving results from time to time."
}

JOURNAL_ID_RE = re.compile('<a href="/journal/(?P<journal_id>\d+)" title=".*?">', re.IGNORECASE)
SEARCH_RESULTS_COUNT_RE = re.compile('<h1 class="number-of-search-results-and-search-terms">\s*<strong>(?P<count>[\d,]+)</strong>', re.IGNORECASE)
SEARCH_RESULTS_TITLE_RE = re.compile('<p class="message">You are now only searching within the Journal</p>\s*<p class="title">\s*<a href="/journal/\d+">(?P<title>.*?)</a>', re.IGNORECASE | re.UNICODE)

SPRINGER_OA_SEARCH = "https://link.springer.com/search?facet-journal-id={}&package=openaccessarticles&search-within=Journal&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"
SPRINGER_FULL_SEARCH = "https://link.springer.com/search?facet-journal-id={}&query=&date-facet-mode=in&facet-start-year={}&facet-end-year={}"

APC_DE_FILE = "apc_de.csv"
OFFSETTING_FILE = "offsetting.csv"

COVERAGE_CACHE_FILE = "coverage_stats.json"
CLASSIFICATOR_CACHE_FILE = "license_classification.json"

COVERAGE_CACHE = {}
CLASSIFICATOR_CACHE = {}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("job", choices=["tables", "model", "yamls", "db_settings", "coverage_stats"])
    parser.add_argument("-d", "--dir", help=ARG_HELP_STRINGS["dir"])
    parser.add_argument("-n", "--num_api_lookups", type=int, help=ARG_HELP_STRINGS["num_api_lookups"])
    args = parser.parse_args()
    
    path = "."
    if args.dir:
        if os.path.isdir(args.dir):
            path = args.dir
        else:
            print "ERROR: '" + args.dir + "' is no valid directory!"
    
    if args.job == "tables":
        if not os.path.isfile("db_settings.ini"):
            print "ERROR: Database Configuration file db_settings.ini not found!"
            sys.exit()
        scp = ConfigParser.SafeConfigParser()
        scp.read("db_settings.ini")
        try:
            db_user = scp.get("postgres_credentials", "user")
            db_pass = scp.get("postgres_credentials", "pass")
        except (ConfigParser.NoSectionError, ConfigParser.NoOptionError) as e:
            print "ERROR: db_settings.ini is malformed ({})".format(e.message)
            sys.exit()
        psql_uri = "postgresql://" + db_user + ":" + db_pass + "@localhost/openapc_db"
        engine = sqlalchemy.create_engine(psql_uri)
        create_cubes_tables(engine, APC_DE_FILE, OFFSETTING_FILE)
        with engine.begin() as connection:
            connection.execute("GRANT SELECT ON ALL TABLES IN SCHEMA openapc_schema TO cubes_user")
        
        
    elif args.job == "model":
        generate_model_file(path)
    elif args.job == "yamls":
        generate_yamls(path)
    elif args.job == "db_settings":
        if os.path.isfile("db_settings.ini"):
            print "ERROR: db_settings.ini already exists"
            sys.exit()
        scp = ConfigParser.SafeConfigParser()
        scp.add_section('postgres_credentials')
        scp.set('postgres_credentials', 'USER', 'table_creator')
        scp.set('postgres_credentials', 'PASS', 'change_me')
        with open('db_settings.ini', 'w') as config_file:
            scp.write(config_file)
    elif args.job == "crossref_stats":
        update_coverage_stats(args.num_crossref_lookups)
        
        
        
def init_table(table, fields, create_id=False):
    
    type_map = {"integer": sqlalchemy.Integer,
                "float": sqlalchemy.Numeric,
                "string": sqlalchemy.String(512),
                "text": sqlalchemy.Text,
                "date": sqlalchemy.Text,
                "boolean": sqlalchemy.Integer}
    
    if create_id:
        col = sqlalchemy.schema.Column('id', sqlalchemy.Integer, primary_key=True)
        table.append_column(col)

    for (field_name, field_type) in fields:
        col = sqlalchemy.schema.Column(field_name, type_map[field_type.lower()])
        table.append_column(col)

    table.create()

def create_cubes_tables(connectable, apc_file_name, offsetting_file_name, schema="openapc_schema"):
    
    apc_fields = [
        ("institution", "string"),
        ("period", "string"),
        ("euro", "float"),
        ("doi", "string"),
        ("is_hybrid", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("issn", "string"),
        ("issn_print", "string"),
        ("issn_electronic", "string"),
        ("issn_l", "string"),
        ("license_ref", "string"),
        ("indexed_in_crossref", "string"),
        ("pmid", "string"),
        ("pmcid", "string"),
        ("ut", "string"),
        ("url", "string"),
        ("doaj", "string"),
        ("country", "string")
    ]
    
    offsetting_fields = [
        ("institution", "string"),
        ("period", "string"),
        ("doi", "string"),
        ("is_hybrid", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("issn", "string"),
        ("issn_print", "string"),
        ("issn_electronic", "string"),
        ("issn_l", "string"),
        ("license_ref", "string"),
        ("indexed_in_crossref", "string"),
        ("pmid", "string"),
        ("pmcid", "string"),
        ("ut", "string"),
        ("url", "string"),
        ("doaj", "string"),
        ("country", "string"),
    ]
    
    offsetting_coverage_fields = [
        ("period", "string"),
        ("publisher", "string"),
        ("journal_full_title", "string"),
        ("is_hybrid", "string"),
        ("num_offsetting_articles", "float"),
        ("num_journal_total_articles", "float"),
        ("num_journal_oa_articles", "float")
    ]

    metadata = sqlalchemy.MetaData(bind=connectable)
    
    openapc_table = sqlalchemy.Table("openapc", metadata, autoload=False, schema=schema)
    if openapc_table.exists():
        openapc_table.drop(checkfirst=False)
    init_table(openapc_table, apc_fields)
    openapc_insert_command = openapc_table.insert()
    
    offsetting_table = sqlalchemy.Table("offsetting", metadata, autoload=False, schema=schema)
    if offsetting_table.exists():
        offsetting_table.drop(checkfirst=False)
    init_table(offsetting_table, offsetting_fields)
    offsetting_insert_command = offsetting_table.insert()
    
    combined_table = sqlalchemy.Table("combined", metadata, autoload=False, schema=schema)
    if combined_table.exists():
        combined_table.drop(checkfirst=False)
    init_table(combined_table, apc_fields)
    combined_insert_command = combined_table.insert()
    
    offsetting_coverage_table = sqlalchemy.Table("offsetting_coverage", metadata, autoload=False, schema=schema)
    if offsetting_coverage_table.exists():
        offsetting_coverage_table.drop(checkfirst=False)
    init_table(offsetting_coverage_table, offsetting_coverage_fields)
    offsetting_coverage_insert_command = offsetting_coverage_table.insert()
    
    # a dict to store individual insert commands for every table
    tables_insert_commands = {
        "openapc": openapc_insert_command,
        "offsetting": offsetting_insert_command,
        "combined": combined_insert_command,
        "offsetting_coverage": offsetting_coverage_insert_command
    }
    
    offsetting_institution_countries = {}
    
    reader = UnicodeReader(open("static/institutions_offsetting.csv", "rb"))
    for row in reader:
        institution_name = row["institution"]
        country = row["country"]
        offsetting_institution_countries[institution_name] = country
        
    crossref_mappings = None
    try:
        cache_file = open(COVERAGE_CACHE_FILE, "r")
        crossref_mappings = json.loads(cache_file.read())
    except IOError as ioe:
        msg = "Error while trying to open crossref cache file {}: {}"
        print msg.format(COVERAGE_CACHE_FILE, ioe)
        sys.exit()
    except ValueError as ve:
        msg = "Error while trying to decode cache structure in {}: {}"
        print msg.format(COVERAGE_CACHE_FILE, ve.message)
        sys.exit()
    
    summarised_offsetting = {}
    issn_title_map = {}
    
    reader = UnicodeReader(open(offsetting_file_name, "rb"))
    for row in reader:
        institution = row["institution"]
        period = row["period"]
        publisher = row["publisher"]
        is_hybrid = row["is_hybrid"]
        issn = row["issn"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        title = row["journal_full_title"]
        try:
            row["country"] = offsetting_institution_countries[institution]
        except KeyError as ke:
            msg = (u"KeyError: The institution '{}' was not found in the institutions_offsetting file!")
            print msg.format(institution)
            sys.exit()
        tables_insert_commands["offsetting"].execute(row)
        if row["euro"] != "NA":
            tables_insert_commands["combined"].execute(row)
        
        issn_title_map[issn] = title
        
        if publisher not in summarised_offsetting:
            summarised_offsetting[publisher] = {}
        if issn not in summarised_offsetting[publisher]:
            summarised_offsetting[publisher][issn] = {}
        if period not in summarised_offsetting[publisher][issn]:
            summarised_offsetting[publisher][issn][period] = 1
        else:
            summarised_offsetting[publisher][issn][period] += 1
    
    for publisher, issns in summarised_offsetting.iteritems():
        for issn, periods in issns.iteritems():
            for period, count in periods.iteritems():
                    row = {
                        "publisher": publisher,
                        "journal_full_title": issn_title_map[issn],
                        "period": period,
                        "is_hybrid": is_hybrid,
                        "num_offsetting_articles": count
                    }
                    try:
                        crossref_info = crossref_mappings[issn][period]
                        row["num_journal_total_articles"] = crossref_info["num_journal_total_articles"]
                        row["num_journal_oa_articles"] = crossref_info["num_journal_oa_articles"]
                    except KeyError as ke:
                        msg = ("KeyError: No crossref statistics found for journal '{}' " +
                               "({}) in the {} period. Update the crossref cache with " +
                               "'python assets_generator.py crossref_stats'.")
                        print msg.format(title, issn, period)
                        sys.exit()
                    tables_insert_commands["offsetting_coverage"].execute(row)
    
    institution_countries = {}
    
    reader = UnicodeReader(open("static/institutions.csv", "rb"))
    for row in reader:
        cubes_name = row["institution_cubes_name"]
        institution_name = row["institution"]
        country = row["country"]
        institution_countries[institution_name] = country
        if institution_name not in tables_insert_commands:
            table = sqlalchemy.Table(cubes_name, metadata, autoload=False, schema=schema)
            if table.exists():
                table.drop(checkfirst=False)
            init_table(table, apc_fields)
            insert_command = table.insert()
            tables_insert_commands[institution_name] = insert_command
    
    reader = UnicodeReader(open(apc_file_name, "rb"))
    for row in reader:
        institution = row["institution"]
        # colons cannot be escaped in URL queries to the cubes server, so we have
        # to remove them here
        row["journal_full_title"] = row["journal_full_title"].replace(":", "")
        row["country"] = institution_countries[institution]
        tables_insert_commands[institution].execute(row)
        tables_insert_commands["openapc"].execute(row)
        tables_insert_commands["combined"].execute(row)

def generate_model_file(path):
    content = u""
    with open("static/templates/MODEL_FIRST_PART", "r") as model:
        content += model.read()
        
    with open("static/templates/MODEL_CUBE_STATIC_PART", "r") as model:
        static_part = model.read()

    reader = UnicodeReader(open("static/institutions.csv", "rb"))
    for row in reader:
        content += u"        ,\n        {\n"
        content += u'            "name": "{}",\n'.format((row["institution_cubes_name"]))
        content += u'            "label": "{} openAPC data cube",\n'.format((row["institution_full_name"]))
        content += static_part
        
    with open("static/templates/MODEL_LAST_PART", "r") as model:
        content += model.read()
    
    output_file = os.path.join(path, "model.json")
    with open(output_file, "w") as model:
        model.write(content.encode("utf-8"))
        
def generate_yamls(path):
    with open("static/templates/YAML_STATIC_PART", "r") as yaml:
        yaml_static = yaml.read()
    
    reader = UnicodeReader(open("static/institutions.csv", "rb"))
    for row in reader:
        content = u"name: " + row["institution_full_name"] + u"\n"
        content += u"slug: " + row["institution_cubes_name"] + u"\n"
        content += u"tagline: " + row["institution_full_name"] + u" APC data\n"
        content += u"source: Open APC\n"
        content += u"source_url: https://github.com/OpenAPC/openapc-de\n"
        content += u"data_url: https://github.com/OpenAPC/openapc-de/blob/master/data/apc_de.csv\n"
        content += u"continent: " + row["continent"] + u"\n"
        content += u"country: " + row["country"] + u"\n"
        content += u"state: " + row["state"] + u"\n"
        content += u"level: kommune\n"
        content += u"dataset: '" + row["institution_cubes_name"] + "'\n"
        content += yaml_static
        
        out_file_name = row["institution_cubes_name"] + ".yaml"
        out_file_path = os.path.join(path, out_file_name)
        with open(out_file_path, "w") as outfile:
            outfile.write(content.encode("utf-8"))
            
        
def _query_springer_link(journal_id, period, oa=False, pause=0.5):
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
        results['count'] = count.replace(",", "")
    else:
        raise ValueError("Regex could not detect a results count at " + url)
    title_match = SEARCH_RESULTS_TITLE_RE.search(content)
    if title_match:
        title = title_match.groupdict()['title']
        htmlparser = HTMLParser()
        results['title'] = htmlparser.unescape(title)
    else:
        raise ValueError("Regex could not detect a journal title at " + url)
    return results        
            
def _get_springer_journal_id_from_doi(doi):
    if doi.startswith(("10.1007/s", "10.3758/s", "10.1245/s", "10.1617/s")):
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
    with open(CLASSIFICATOR_CACHE_FILE, "w") as f:
        f.write(json.dumps(CLASSIFICATOR_CACHE, sort_keys=True, indent=4, separators=(',', ': ')))
        f.flush()
    print "Done."
    sys.exit()
    
def update_coverage_stats(max_lookups):
    global COVERAGE_CACHE, CLASSIFICATOR_CACHE
    if os.path.isfile(COVERAGE_CACHE_FILE):
        with open(COVERAGE_CACHE_FILE, "r") as f:
            try:
               COVERAGE_CACHE  = json.loads(f.read())
               print "coverage cache file sucessfully loaded."
            except ValueError:
                print "Could not decode a cache structure from " + COVERAGE_CACHE_FILE + ", starting with an empty coverage cache."
    else:
        print "No cache file (" + COVERAGE_CACHE_FILE + ") found, starting with an empty coverage cache."
    if os.path.isfile(CLASSIFICATOR_CACHE_FILE):
        with open(CLASSIFICATOR_CACHE_FILE, "r") as f:
            try:
               CLASSIFICATOR_CACHE  = json.loads(f.read())
               print "Classificator cache file sucessfully loaded."
            except ValueError:
                print "Could not decode a cache structure from " + CLASSIFICATOR_CACHE_FILE + ", starting with an empty classificator cache."
    else:
        print "No cache file (" + CLASSIFICATOR_CACHE_FILE + ") found, starting with an empty classificator cache."
    
    reader = UnicodeReader(open(OFFSETTING_FILE, "r"))
    num_lookups = 0
    for line in reader:
        publisher = line["publisher"]
        if publisher != "Springer Nature":
            continue
        year = line["period"]
        issn = line["issn"]
        title = line["journal_full_title"]
        doi = line["doi"]
        try:
            _ = COVERAGE_CACHE[issn][year]["num_journal_total_articles"]
            _ = COVERAGE_CACHE[issn][year]["num_journal_oa_articles"]
        except KeyError:
            msg = u'No cached entry found for journal "{}" ({}) in the {} period, querying SpringerLink...'
            print msg.format(title, issn, year)
            journal_id = _get_springer_journal_id_from_doi(doi)
            total_res = _query_springer_link(journal_id, year, oa=False)
            oa_res = _query_springer_link(journal_id, year, oa=True)
            result = {
                "num_journal_total_articles": total_res["count"],
                "num_journal_oa_articles": oa_res["count"]
            }
            #content = _query_crossref(issn, year)
            #result = _analyse_crossref_data(content)
            if issn not in COVERAGE_CACHE:
                COVERAGE_CACHE[issn] = {}
            COVERAGE_CACHE[issn][year] = result
            num_lookups += 1
            if max_lookups is not None and num_lookups >= max_lookups:
                print "maximum number of lookups performed."
                _shutdown()
    _shutdown()
    

if __name__ == '__main__':
    main()
