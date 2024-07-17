# DataQuery

DataQuery is a web app for preview, query, edit and export data files.

DataQuery provides a Stremlit based Web UI and a SQL Engine using DuckDb.

App is available on Streamlit [here](https://dataquery.streamlit.app/)

## Features

- Load files of various formats (csv, txt, xlsx, parquet, avro, json, xml). Files can be loaded also as zip archives.
- Preview data of every file loaded (in case of xlsx file, every sheet will be avaiable)
- Query loaded data in sql language
- Visual editing of the query results
- Export query results on file, in various format (csv, txt, xlsx, parquet, avro, json, xml)

All the tasks are managed in memory, no data are saved on the server.

## Installation

To install the required dependencies you can use `pip`:

```sh
pip install -r requirements.txt
```

## Run

To run the the app you can use `streamlit`:

```sh
streamlit run dataquery.py
```

## License

This project is licensed under the GNU GPLv3 License. See the LICENSE file for details.
