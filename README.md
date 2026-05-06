# DataQuery

A web app for preview, query, edit and export data files.

**DataQuery** provides a *Streamlit* based Web UI and a SQL Engine using *DuckDb*.

Try it on *Streamlit*: [here](https://dataquery.streamlit.app/).

## Features

- **Load data files of various formats (avro, csv, json, parquet, txt, xlsx, xml)**  
  Files can be loaded also as zip archives. For xlsx files, select which sheets to load and optionally assign custom table aliases. For csv, txt and xlsx files, header, delimiter and quoting settings can be defined  
- **Preview data of every file loaded**  
  Get a preview of the data loaded and, if needed, refine data loading settings  
- **Query loaded data in sql language**  
  Join, transform, analyze data in a relational in-memory database  
- **Visual editing of the query results**  
  Edit your data applying changes directly in the table that shows you query result  
- **Export query results on file, in various format (avro, csv, json, parquet, txt, xlsx, xml)**  
  For csv, txt and xlsx file define setting for header, delimiter and quoting settings  

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
