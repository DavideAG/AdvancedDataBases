To complete the task I used a Jupyther notebook based on Anaconda.
In ingest_data_and_requests.ipynb there are the information
related to the ingest phase and the bodies of the queries.

A readable version (with results) of this document is also
provided in two different file:
	ingest_data_and_requests.html	(html)
	ingest_data_and_requests.md	(markdown)

The data ingestion is concurrent, I used a split of two threads
to push into ES the dataset. In this way I reduced the load time.