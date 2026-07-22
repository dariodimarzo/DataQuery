# DataQuery

A web app for preview, query, edit and export data files.

**DataQuery** provides a *Streamlit* based Web UI and a SQL Engine using *DuckDb*.

Try it on *Streamlit*: [here](https://dataquery.streamlit.app/).

## Features

- **Load data files of various formats (avro, csv, json, parquet, txt, xlsx, xml)**  
  Files can be loaded also as zip archives. 
  Each file has a *Load this file* toggle to include or exclude it individually (useful to drop single files from a zip archive). 
  For xlsx files, select which sheets to load and optionally assign custom table aliases. 
  For csv, txt and xlsx files, header, delimiter, quoting settings and table alias can be defined.  
- **Preview data of every file loaded**  
  Get a preview of the data loaded and, if needed, refine data loading settings  
- **Query loaded data in sql language**  
  Join, transform, modify, analyze data in a relational in-memory database  
- **Visual editing of the query results**  
  Edit your data applying changes directly in the table that shows you query result  
- **Export query results on file, in various format (avro, csv, json, parquet, txt, xlsx, xml)**  
  For csv, txt and xlsx file define setting for header, delimiter and quoting settings  
- **Save query results as a session table**  
  After running a query, save its result (including manual edits) as a new named table, reusable in further queries  
- **Save the whole session**  
  Download all session tables as a single ZIP of parquet files. The bundle can be re-uploaded to restore the session  

All the tasks are managed in memory, no data are saved on the server.  

## Installation

Install the package:

```sh
pip install .
```

Or install in editable mode for development:

```sh
pip install -e .
```

## Run

Run with the console entry point:

```sh
dataquery
```

Or with Streamlit directly:

```sh
streamlit run src/dataquery/app.py
```

## License

This project is licensed under the GNU GPLv3 License. See the LICENSE file for details.
