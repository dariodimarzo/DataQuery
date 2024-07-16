# DataQuery

DataQuery is a web app to upload data files for preview, query, edit and export data.

DataQuery provides a Stremlit based Web UI and a SQL Engine using DuckDb.

## Features

- Load files of various formats (Csv, Txt, Xlsx, Parquet, Avro, Json, Xml..). Files can be loaded also as zip archives.
- Preview data of every file loaded (in case of xlsx file, every sheet will be avaiable)
- Query loaded data in sql language
- Visual editing of the query results
- Export query results on file, in various format (Csv, Xlsx, Parquet, Json, Xml)

## Installation

To install the required dependencies, you can use `pip`:

```sh
pip install -r requirements.txt
```

## Run

To run the the app you can use Streamlit

```sh
streamlit run dataquery.py
```

## License

This project is licensed under the GNU GPLv3 License. See the LICENSE file for details.