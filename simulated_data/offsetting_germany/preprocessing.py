#!/usr/bin/env python

import csv
from os import path
import sys
        
SPRINGER_CATALOGUE_FILES = {
    "2016": "../../springer_journal_lists/2016.csv",
    "2017": "../../springer_journal_lists/2017.csv"
}

OFFSETTING_FILE = "../../offsetting.csv"

OPEN_CHOICE_ISSNS = {}
OFFSETTING_DOIS = {}

def main():
    
    for year, catalogue_file in SPRINGER_CATALOGUE_FILES.items():
        OPEN_CHOICE_ISSNS[year] = []
        with open(catalogue_file) as f:
            reader = UnicodeReader(f)
            for line in reader:
                if line["Open Access Option"] == "Hybrid (Open Choice)":
                    for issn_type in ["ISSN print", "ISSN electronic"]:
                        if len(line[issn_type]):
                            OPEN_CHOICE_ISSNS[year].append(line[issn_type])
                            
    with open(OFFSETTING_FILE) as f:
        reader = UnicodeReader(f)
        for line in reader:
            OFFSETTING_DOIS[line["doi"]] = line["institution"]
    
    
    modified_content = [] 
    delete_stats = {
        "no_doi": 0,
        "no_oc_issn": 0,
        "duplicate": 0,
        "nature_doi": 0
    }
    deleted_issns = {}
    deleted_duplicates = {}
    
    with open("springer_pub_non_oa.csv") as in_file:
        reader = UnicodeReader(in_file)
        for line in reader:
            year = line["period"]
            issn = line["issn"]
            doi = line["doi"]
            if len(doi) == 0:
                delete_stats["no_doi"] += 1
                continue
            if issn not in OPEN_CHOICE_ISSNS[year]:
                delete_stats["no_oc_issn"] += 1
                if issn not in deleted_issns:
                    deleted_issns[issn] = {"example_doi": "https://doi.org/" + doi, "count": 1}
                else:
                    deleted_issns[issn]["count"] += 1
                continue
            if doi.startswith("10.1038"):
                delete_stats["nature_doi"] += 1
            if doi in OFFSETTING_DOIS:
                delete_stats["duplicate"] += 1
                institution = OFFSETTING_DOIS[doi]
                if institution not in deleted_duplicates:
                    deleted_duplicates[institution] = {"example_doi": "https://doi.org/" + doi, "count": 1}
                else:
                    deleted_duplicates[institution]["count"] += 1
                continue
            modified_content.append(line)
    
        with open("out.csv", "w") as out_file:
            writer = csv.DictWriter(out_file, fieldnames=reader.reader.fieldnames, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            for line in modified_content:
                writer.writerow(line)
        
        print "\n----- Deleted entries based on ISSN not belonging to an Open Choice journal-----\n"
        keys = deleted_issns.keys()
        sorted_keys = sorted(keys, key=lambda x: deleted_issns[x]["count"], reverse=True)
        for key in sorted_keys:
            print "{}: {}   (example DOI: {})".format(key, deleted_issns[key]["count"], deleted_issns[key]["example_doi"])
        print "Entries from {} distinct journals removed (no Open Choice ISSN)".format(len(deleted_issns.keys()))
        print "\n----- Deleted entries based on duplicate DOI in offsetting data set -----\n"
        keys = deleted_duplicates.keys()
        sorted_keys = sorted(keys, key=lambda x: deleted_duplicates[x]["count"], reverse=True)
        for key in sorted_keys:
            print "{}: {}   (example DOI: {})".format(key, deleted_duplicates[key]["count"], deleted_duplicates[key]["example_doi"])
        print "Entries from {} distinct institution(s) removed (DOI duplicates with offsetting data)".format(len(deleted_duplicates.keys()))

        deleted_total = reduce(lambda x, y: x + y, delete_stats.values())
        msg = "\nout file created, deleted {} lines ({} without DOI, {} lines with a Nature DOI, {} no Open Choice ISSN, {} duplicates with existing offsetting data)"
        msg = msg.format(deleted_total, delete_stats["no_doi"], delete_stats["nature_doi"], delete_stats["no_oc_issn"], delete_stats["duplicate"])
        print colorise(msg, "green")

if __name__ == '__main__' and __package__ is None:
    sys.path.append(path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
    from util import UnicodeReader, colorise
    main()
