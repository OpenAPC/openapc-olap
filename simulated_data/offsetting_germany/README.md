

## About

This directory contains _simulated_ offsetting data. Its intention is to calculate the hypothetical impact of a national Springer Compact agreement for Germany in the years 2016 and 2017 (at the moment the Max Planck Society is the only German institution taking part in the programme).
To do this, our colleagues at the OA analytics group performed a bibliometrical analysis and created a raw data set with the following parameters:

- RP author at a German institution
- published with Springer Nature
- not a Gold OA journal
- publication year 2016 or 2017

## Preprocessing:

To make the data compatible with our exisiting offsetting data, the following preprocessing steps are taken out:

- Remove all articles lacking a DOI. This leads to some inaccuracies, but we cannot process such articles using our OpenAPC enrichment tools.
- Remove all articles where the journal ISSN does not belong to a Springer Open Choice journal in the according year.
- Remove all articles where the DOI is already present in our offsetting data set.
- Remove all articles where the DOI has a Nature prefix (10.1038). This also leads to slight inaccuracies, but unfortunately Nature journals are not hosted on SpringerLink, which means that we cannot obtain metrics (Number of OA/total articles) for them.

In additon, all articles were looked up on SpringerLink to determine if they are already Open Access - this is necessary since those articles would not affect the simulated journal OA statistics.

The preprocessing is performed using a [python script](preprocessing.py), a [log file](preprocessing_log.txt) gives details on how many articles were deleted during the process. The script creates two output files, one for OA and one for non-OA articles.

