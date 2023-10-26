## Introduction

The OpenAPC [OLAP server](https://olap.openapc.net) is the data back end for the [treemap visualisations](https://treemaps.openapc.net) and can also be used as a public API to access the OpenAPC data. Querying the OLAP might be a convenient middle way if you don't want to process the raw CSV file first, but need more functionality than the treemaps can offer.

The server is based on [cubes](https://pythonhosted.org/cubes/), and its documentation applies to the OpenAPC variant as well (you will probably be using the [aggregate](https://pythonhosted.org/cubes/server.html#aggregate) function most often).

If you are not familiar with OLAP and its cube model concept yet, you might want to have a look at the [Wikipedia article](https://en.wikipedia.org/w/index.php?title=OLAP_cube&oldid=900627823) for a quick overview.

## Preface

Before we get started, there are are 3 things to keep in mind:

1) The server return format is JSON, which is machine-readable, but not really human-readable (at least if not pretty-printed). If you want to view the results directly in your web browser, it's highly recommended to install an extension which properly formats JSON (Like [JSON Lite](https://github.com/lauriro/json-lite) for Firefox and Chrome).

2) For performance reasons the OLAP server makes use of _pagination_, meaning that large result sets are split into smaller units and then served on multiple server pages. The maximum number of items which can be returned on a single page is 500. It is important to note that pagination is __not turned on automatically__! This means that if you make a query to the OLAP server and the answer contains exactly 500 entries, the result is probably incomplete and you have to tell the server to make use of pagination to obtain the missing items. This is done by adding two parameters to the query URL, `pagesize` and `page`, like this: `&pagesize=500&page=3` (You have to use both parameters, adding just one of them won't have any effect). `pagesize` is the return size of a single page, and there's rarely any reason to set this to anything less than the allowed 500 items. `page` is the number of the results page to get, starting at 0. In practice you would iterate over increasing page numbers until a result is empty or not filled up to the maximum page size. Which brings us directly to the last point:

3) Performance, part 2. Whenever making heavy use of the OLAP server, especially in scripted scenarios, be gentle. Our ressources, both in terms of bandwidth and computational power, are limited, so please try to avoid putting a strain on them. Store/cache intermediate results and add a sleeping interval of at least one second to your scripts when performing multiple queries.

## General Usage

Each participating institution in OpenAPC has its data stored in their own OLAP cube, but there are also a number of aggregated ones which correspond to certain OpenAPC data sets: 

|cube name| description |
|---------|-------------|
| *openapc* | OpenAPC core data set, contains cost data on APCs |
| *bpc* | BPC data set, contains cost data on OA monographs/books |
| *transformative_agreements* | Contains metadata on articles published under transformative agreements (TAs). Note that the records in here usually do not have cost data assigned. |
| *combined* | Combines the *openapc* cube with those articles from *transformative_agreements* which have cost data assigned. |


This query provides a list of all existing cubes:

1. <https://olap.openapc.net/cubes>

The most basic operation is to list all entries (or _cells_ in OLAP parlance) contained in a singular cube:

2. <https://olap.openapc.net/cube/bielefeld_u/facts>

As we can see, the results page is cut at 500 entries, so we have to activate pagination to obtain all of them:

3. <https://olap.openapc.net/cube/bielefeld_u/facts?pagesize=500&page=0>
4. <https://olap.openapc.net/cube/bielefeld_u/facts?pagesize=500&page=1>

The most interesting function of an OLAP server is the use of _aggregates_. Aggregates are functions which can be applied to a certain set of cells, calculating a single result. The OpenAPC OLAP server implements four  different aggregate functions:

- apc_num_items (Article count)
- apc_amount_sum (Summarised APC)
- apc_amount_avg (APC Arithmetic Mean)
- apc_amount_stddev (APC standard deviation)

In its most simple form, an aggregate call will pull together all cells in a cube and apply all functions to them:

5. <https://olap.openapc.net/cube/bielefeld_u/aggregate>

Since we operate on the cube belonging to Bielefeld University, all numbers will relate to the total amount of articles contributed by this institution.

More interesting, however, is the application of _drilldowns_ and _cuts_ to the data prior to aggregation. A drilldown is executed along a certain _dimension_ and will partition the cube into subsets. For example, if we are interested how the total APC cost amount we had obtained for Bielefeld University in the previous query is distributed over different publishers, we may apply a drilldown along the _publisher_ dimension:

6. <https://olap.openapc.net/cube/bielefeld_u/aggregate?drilldown=publisher>

This will create a subset for every publisher Bielefeld University has published at least one article with and then apply the 4 aggregate functions to each of them. The aggregation results can also be used for ordering: For example, if want to sort the results according to the APC amount paid to each publisher, we can do the following:

7. <https://olap.openapc.net/cube/bielefeld_u/aggregate?drilldown=publisher&order=apc_amount_sum>

It's also possible to drill down along more than one dimension. In the following example we repeat our publisher drilldown from the previous example, followed by a drilldown along the _journal_full_title_ dimension. By doing this we split the publisher partitions into even smaller units by creating a subset for each existing publisher/journal combination:

8. <https://olap.openapc.net/cube/bielefeld_u/aggregate?drilldown=publisher|journal_full_title>

If you want to know which dimensions are available for drilldown, you may examine a cube's model and have a look at the _dimensions_ key:

9. <https://olap.openapc.net/cube/bielefeld_u/model>

Drilling down will subset a cube, but it will never reduce the amount of cells. If we want to exempt certain data from showing up, we can apply a _cut_:

10. <https://olap.openapc.net/cube/openapc/aggregate?cut=institution:Bielefeld%20U>

We are now working with the full OpenAPC data set (cube "openapc"), but then applying a cut reducing it to those articles where the institution is "Bielefeld U", so the result is equivalent to query number 5. If the dimension we use for cutting is numerical, we can also specify a range (In the OpenAPC data model, _period_ is the only dimension where this is possible):

11. <https://olap.openapc.net/cube/openapc/aggregate?cut=period:2014~2016>

This shows APC expenditures which occured between 2014 to 2016. It's also possible to combine cuts and drilldowns:

12. <https://olap.openapc.net/cube/openapc/aggregate?drilldown=institution&cut=country:DEU>

Here we are creating an overview of the institutions participating in OpenAPC, but reducing it to those located in Germany.

All of the methods above can be combined freely, which can make OLAP queries both powerful and complex:

13. <https://olap.openapc.net/cube/openapc/aggregate?drilldown=publisher|institution&cut=country:GBR|is_hybrid:TRUE&order=apc_num_items&pagesize=500&page=0>

What's happening here? We're creating a 2-level drilldown along the publisher and institution dimensions, so we will see a partition showing how much each publisher has received from each institution. However, we are also applying two cuts which reduce the result to institutions based in the United Kingdom and to articles published in hybrid journals. We are then ordering the results according to the number of articles and since the result set is large, we are also requesting pagination. Conclusion? If we iterate to the last page, we can find the one British institution in our data set which has published the most articles in hybrid journals with one given publisher.

## DOI Lookup

Another common use case for the OLAP server is to look up if a certain DOI is present in OpenAPC. For this purpose there's a special cube *doi_lookup* which holds all DOIs from the 3 main OpenAPC data sets. A doi lookup is possible by applying the DOI as cut function to a facts listing of this cube: 

14. https://olap.openapc.net/cube/doi_lookup/facts?cut=doi:10.3389/fmicb.2020.589364

If the DOI is present this will return a cell with basic record metadata, including an `url` field which contains a link to the original cell. If the DOI is not present in OpenAPC, an empty result (`[]`) is returned.  There are two things to keep in mind when using this kind of query, however:
    
- The DOI must be given in OpenAPC-normalized form, meaning:
    - It has to be a "pure" DOI, e.g starting with the `10.` prefix. Other notations like the DOI handbook format (`doi:10.xxx`) or URLs (`doi.org/10.xxx`, `http://dx.doi.org/10.xxx`) won't return any results.
    - Although DOI names are generally case insensitive, OpenAPC normalizes them to all lower case during processing. Since the OLAP URL scheme is case sensitive, **query DOIs have to be converted to all lower case**! Example: https://olap.openapc.net/cube/openapc/facts?cut=doi:10.3389/fmicb.2020.589364 vs https://olap.openapc.net/cube/openapc/facts?cut=doi:10.3389/FMICB.2020.589364
- There are a small number of records in OpenAPC without a DOI, these cannot be obtained via this method. This is usually more of a problem within the BPC data set, as DOI assignment is a less common practice for OA monographs compared to journal articles. If you want to look up a larger list of OA books, the better approach is probably to obtain our BPC data set as [CSV file](https://github.com/OpenAPC/openapc-de/blob/master/data/bpc.csv) and include a title/ISBN search. 
