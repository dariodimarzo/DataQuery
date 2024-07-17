import streamlit as st
import pandas as pd
import duckdb
import io
import base64
import re
import os
import avro.schema
from avro.datafile import DataFileReader
from avro.io import DatumReader
#from typing import List, Optional
import zipfile
import csv
import lxml
import html5lib


def load_file(file, con):
    """
    Load a file into a DataFrame and register it as a table in the database connection.

    Args:
        file (file-like object): The file to load.
        con (database connection): The database connection to register the DataFrame as a table.

    Returns:
        list: A list of table names that were registered.

    Raises:
        ValueError: If the file format is not supported.
        Exception: If there is an error loading the file.

    """
    file_extension = file.name.split('.')[-1].lower()
    file_nm = f"{os.path.splitext(file.name)[0]}"
    table_names = []

    try:
        if file_extension in ['csv', 'txt']:
            # Read a small sample of the file to detect the dialect
            sample = file.read(2048)
            file.seek(0)  # Reset file pointer
            dialect = csv.Sniffer().sniff(sample.decode('utf-8'))
            
            # Use the detected dialect to read the CSV
            df = pd.read_csv(file, 
                             sep=dialect.delimiter, 
                             quotechar=dialect.quotechar,
                             escapechar=dialect.escapechar,
                             skipinitialspace=dialect.skipinitialspace)
            
            table_names.append(register_dataframe(con, df, file_nm))
        elif file_extension == 'xlsx':
            xls = pd.ExcelFile(file)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(file, sheet_name=sheet_name)
                table_names.append(register_dataframe(con, df, f"{file_nm}_{sheet_name}"))
        elif file_extension == 'parquet':
            df = pd.read_parquet(file)
            table_names.append(register_dataframe(con, df, file_nm))
        elif file_extension == 'avro':
            avro_reader = DataFileReader(file, DatumReader())
            df = pd.DataFrame.from_records([r for r in avro_reader])
            table_names.append(register_dataframe(con, df, file_nm))
        elif file_extension in ['json', 'html', 'xml', 'hdf', 'feather', 'pickle', 'sas', 'stata', 'spss']:
            read_func = getattr(pd, f'read_{file_extension}')
            df = read_func(file)
            table_names.append(register_dataframe(con, df, file_nm))
        else:
            raise ValueError(f"Unsupported file format: {file_extension}")

        return table_names
    except Exception as e:
        st.error(f"Error loading file {file.name}: {str(e)}")
        return None

def register_dataframe(con, df, file_name):
    """
    Register a DataFrame in the connection object.

    Args:
        con (Connection): The connection object to register the DataFrame.
        df (pd.DataFrame): The DataFrame to register.
        file_name (str): The name of the file (used to generate the table name).

    Returns:
        str: The name of the registered table.
    """
    table_name = clean_table_name(os.path.splitext(file_name)[0])
    con.register(table_name, df)
    return table_name


def clean_table_name(name):
    """
    Cleans the table name by replacing spaces with underscores and removing all other special characters.

    Args:
        name (str): The table name to be cleaned.

    Returns:
        str: The cleaned table name.
    """
    # Replace spaces with underscores
    name = name.replace(' ', '_')
    # Remove all other special characters
    name = re.sub(r'[^a-zA-Z0-9_]', '', name)
    
    return name


def run_query(con, sql_query):
    """
    Executes the given SQL query on the provided connection object and returns the result.

    Parameters:
    con (connection): The connection object to the database.
    sql_query (str): The SQL query to be executed.

    Returns:
    result (DataFrame): The result of the SQL query as a DataFrame.
    """
    result = con.execute(sql_query).fetchdf()
    return result


def preview_data(con, table_name, num_rows=5):
    """
    Preview the data for a given table.

    Args:
        con (Connection): The DuckDB connection object.
        table_name (str): The name of the table to preview.
        num_rows (int): The number of rows to preview.

    Returns:
        pandas.DataFrame: A DataFrame containing the preview data.
    """
    query = f"SELECT * FROM {table_name} LIMIT {num_rows}"
    df = con.execute(query).fetchdf()
    # Reset index to start from 1
    df.index = range(1, len(df) + 1)
    return df

def remove_view(con, view_name):
    """
    Safely remove a view from the DuckDB connection.

    Args:
        con (Connection): The DuckDB connection object.
        view_name (str): The name of the view to remove.
    """
    try:
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
    except duckdb.CatalogException as e:
        st.warning(f"Could not drop view {view_name}: {str(e)}")



def df_to_file(df, file_format, **kwargs):
    """
    Convert DataFrame to various file formats.
    
    Args:
        df (pd.DataFrame): The DataFrame to convert.
        file_format (str): The desired file format.
        **kwargs: Additional arguments for specific file formats.
    
    Returns:
        bytes: The DataFrame converted to the specified format.
    """
    buffer = io.BytesIO()
    
    try:
        if file_format == 'csv':
            try:
                df.to_csv(buffer, index=False, **kwargs)
            except csv.Error as e:
            # Catch specific CSV writing errors
                if "need to escape" in str(e):
                    st.warning("Special character found in the data.  \nPlease select a different quoting option.")
                else:
                    raise ValueError(f"{file_format} writing error: {e}")
        elif file_format == 'excel':
            df.to_excel(buffer, index=False, engine='openpyxl', **kwargs)
        elif file_format == 'json':
            df.to_json(buffer, orient='records', **kwargs)
        elif file_format == 'parquet':
            df.to_parquet(buffer, index=False, engine='pyarrow', **kwargs)   
        elif file_format == 'xml':
            df.to_xml(buffer, index=False, **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    except Exception as e:
        st.warning(f"{file_format} export not available for your data.  \nPlease select a different format.")
    
    buffer.seek(0)
    return buffer.getvalue()


def main():
    """
    The main function of the DataQuery application.
    
    This function handles the main logic of the application, including file uploading, data loading, data preview, SQL query execution, and result visualization.
    It also provides options for editing and downloading query results.
    """

    st.set_page_config(page_title='DataQuery', page_icon=':wavy_dash:', layout="centered")
    st.title("DataQuery")
    st.subheader("Preview, query, edit and export data files")

    # Initialize session state
    if 'query_result' not in st.session_state:
        st.session_state.query_result = None
    if 'tables' not in st.session_state:
        st.session_state.tables = {}
    if 'con' not in st.session_state:
        st.session_state.con = duckdb.connect(database=':memory:')
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = []
    if 'edited_df' not in st.session_state:
        st.session_state.edited_df = None
    if 'export_df' not in st.session_state:
        st.session_state.export_df=None

    uploaded_files = st.file_uploader("Choose data files", accept_multiple_files=True,
        help='Upload your data files and zip archives.  \nAll files from zip archives and all sheets of xlsx files will be considered.')

    # Check for removed files
    removed_files = [file for file in st.session_state.uploaded_files if file not in uploaded_files]
    for file in removed_files:
        tables_to_remove = [table for table, source in st.session_state.tables.items() if source == file.name]
        for table in tables_to_remove:
            remove_view(st.session_state.con, table)
            del st.session_state.tables[table]
        st.warning(f"Removed file: {file.name} and its associated tables")

    # Update the list of uploaded files
    st.session_state.uploaded_files = uploaded_files
    
    if uploaded_files:
        loaded_tab=""
        for file in uploaded_files:
            if file.name not in [f.name for f in st.session_state.uploaded_files if f != file]:
                if file.type == "application/x-zip-compressed":
                    with zipfile.ZipFile(file) as z:
                        for zip_info in z.infolist():
                            with z.open(zip_info) as zf:
                                # Create a new file-like object for each file extracted from the zip
                                extracted_file = io.BytesIO(zf.read())
                                extracted_file.name = zip_info.filename
                                loaded_tables = load_file(extracted_file, st.session_state.con)
                                if loaded_tables:
                                    for table in loaded_tables:
                                        st.session_state.tables[table] = extracted_file.name
                                    loaded_tab += f"Loaded {file.name} - {extracted_file.name} as table(s): {', '.join(loaded_tables)}  \n"
                                        #st.success(f"Loaded {file.name} - {extracted_file.name} as table(s): {', '.join(loaded_tables)}")                           
                else:
                    loaded_tables = load_file(file, st.session_state.con)
                    if loaded_tables:
                        for table in loaded_tables:
                            st.session_state.tables[table] = file.name
                        loaded_tab += f"Loaded {file.name} as table(s): {', '.join(loaded_tables)}  \n"
                            #st.success(f"Loaded {file.name} as table(s): {', '.join(loaded_tables)}")
        
        if loaded_tab != "":
            st.success(loaded_tab)
       

        # Collapsible Data Preview
        if st.session_state.tables:
            with st.expander("Data Preview", expanded=False):
                tabs = st.tabs(list(st.session_state.tables.keys()))
                for i, tab in enumerate(tabs):
                    with tab:
                        table_name = list(st.session_state.tables.keys())[i]
                        preview_df = preview_data(st.session_state.con, table_name)
                        st.dataframe(preview_df)
                        #st.text(f"Source file: {st.session_state.tables[table_name]}")
                        #st.text(f"Showing first 5 rows of {st.session_state.tables[i]}")

            st.subheader("Query Data")
            sql_query = st.text_area("Enter your SQL query:", height=100, key="sql_input")

            if st.button("Run Query"):
                if sql_query:
                    try:
                        st.session_state.query_result=None
                        st.session_state.edited_df=None
                        result_df = run_query(st.session_state.con, sql_query)
                        # Reset index to start from 1 for query results as well
                        result_df.index = range(1, len(result_df) + 1)
                        st.session_state.query_result = result_df
                        st.success("Query executed successfully!")
                    except Exception as e:
                        if "Catalog Error: Table with name" in str(e):
                            st.error("Table not existing. Please check table names in your query.")
                        elif "Can only update base table" in str(e):
                            st.error("Update not available. Please consider a different select statement and the edit mode.")
                        else:
                            st.error(f"Error executing query: {str(e)}")
                else:
                    st.warning("Please enter a SQL query.")         

        # Display query result and download options
        if st.session_state.query_result is not None:
            st.subheader("Query Result")
             # Add a toggle for edit mode
            edit_mode = st.toggle("Edit Mode")
            if edit_mode:
                st.session_state.edited_df = st.data_editor(st.session_state.query_result, num_rows="dynamic")
            else:
                if st.session_state.edited_df is not None:
                    st.session_state.query_result=st.session_state.edited_df.copy()
                st.dataframe(st.session_state.query_result)
            
            #Prepare dataframe for export data
            if st.session_state.edited_df is not None:
                st.session_state.export_df=st.session_state.edited_df.copy()
            else:
                st.session_state.export_df=st.session_state.query_result.copy()

            col1, col2 = st.columns(2)
            with col1:
                with st.popover("Data Download"):
                    # File format selection
                    file_formats = ['csv', 'excel', 'json', 'parquet', 'xml']
                    selected_format = st.selectbox("Select file format:", file_formats)
                    
                    # Delimiter and quoting options (for CSV)
                    if selected_format == 'csv':
                        header = st.selectbox("Header:",("Y", "N"))
                        delimiter = st.text_input("Delimiter:", max_chars=1, value=",")
                        quoting_options = {
                            'QUOTE_ALL': csv.QUOTE_ALL,
                            'QUOTE_MINIMAL': csv.QUOTE_MINIMAL,
                            'QUOTE_NONNUMERIC': csv.QUOTE_NONNUMERIC,
                            'QUOTE_NONE': csv.QUOTE_NONE
                        }
                        quoting = st.selectbox("Quoting:", list(quoting_options.keys()))                    
                        head=True if header == 'Y' else False

                        file_content = df_to_file(st.session_state.export_df, 'csv', 
                                                sep=delimiter, quoting=quoting_options[quoting],header=head)
                    elif selected_format =='excel':
                        header = st.selectbox("Header:",("Y", "N"))
                        head=True if header == 'Y' else False
                        file_content = df_to_file(st.session_state.export_df, 'excel',header=head)
                    else:
                        file_content = df_to_file(st.session_state.export_df, selected_format)
                    

                    # Generate file name and MIME type
                    file_extension = 'xlsx' if selected_format == 'excel' else selected_format
                    file_name = f"query_result.{file_extension}"
                    mime_types = {
                        'csv': 'text/csv',
                        'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                        'json': 'application/json',
                        'parquet': 'application/octet-stream',
                        'xml': 'application/xml'
                    }
                    mime_type = mime_types.get(selected_format, 'application/octet-stream')
                    
                    # Download button
                    st.download_button(
                        label=f"Download as {selected_format.upper()}",
                        data=file_content,
                        file_name=file_name,
                        mime=mime_type,
                    )
    else:
        st.session_state.tables = {}
        st.session_state.query_result = None
        st.session_state.export_df= None

if __name__ == "__main__":
    main()
