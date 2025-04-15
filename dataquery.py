import streamlit as st
import pandas as pd
import duckdb 
from io import BytesIO
from re import sub
from os.path import splitext, basename
import zipfile
import csv
import pandavro as pdx
import json  
from code_editor import code_editor  


def upload_files(file_ext):
    """
    Uploads data files and zip archives.

    Args:
        file_ext (list): A list of file extensions to filter the uploaded files.

    Returns:
        list: A list of uploaded files stored in `st.session_state.uploaded_files`.

    Notes:
        - Handles file uploads via Streamlit's file uploader.
        - Removes views from the database for files that are no longer uploaded.
    """
    # Upload files object
    uploaded_files = st.file_uploader(
        "Choose data files",
        accept_multiple_files=True,
        help='Upload your data files and zip archives.  \nAll files from zip archives and all sheets of xlsx files will be considered.',
        type=file_ext,
    )

    # Check for removed files
    removed_files = [file for file in st.session_state.uploaded_files if file not in uploaded_files]
    # Remove views from the database for removed files
    for file in removed_files:
        tables_to_remove = [table for table, source in st.session_state.tables.items() if source == file.name]
        for table in tables_to_remove:
            remove_view(st.session_state.con, table)
            del st.session_state.tables[table]
        st.warning(f"Removed file: {file.name} and its associated tables")

    # Update the list of uploaded files
    st.session_state.uploaded_files = uploaded_files

    return st.session_state.uploaded_files


def remove_view(con, view_name):
    """
    Safely removes a view from the DuckDB connection.

    Args:
        con (duckdb.Connection): The DuckDB connection object.
        view_name (str): The name of the view to remove.

    Returns:
        None

    Notes:
        - If the view does not exist or cannot be dropped, a warning is displayed.
    """
    try:
        con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
    except duckdb.CatalogException as e:
        st.warning(f"Could not drop view {view_name}: {str(e)}")


def files_to_db(file_ext):
    """
    Loads files into a DuckDB database and registers them as tables.

    Args:
        file_ext (list): A list of file extensions to be loaded.

    Returns:
        dict: A dictionary mapping table names to file names.

    Notes:
        - Logs messages about successfully loaded tables and excluded files.
        - Handles zip archives and extracts supported files for loading.
        - Registers tables in the DuckDB connection.
    """
    loaded_tab = ""
    excluded_tab = ""
    file_options = {}

    for file in st.session_state.uploaded_files:
        if file.name not in [f.name for f in st.session_state.uploaded_files if f != file]:
            file_extension = file.name.split('.')[-1].lower()

            # Manage zip archives
            if file.type == "application/x-zip-compressed":
                with zipfile.ZipFile(file) as z:
                    for zip_info in z.infolist():
                        if not zip_info.is_dir():
                            _, extension = splitext(zip_info.filename)
                            if extension.lstrip('.').lower() in file_ext:
                                with z.open(zip_info) as zf:
                                    extracted_file = BytesIO(zf.read())
                                    extracted_file.name = basename(zip_info.filename)
                                    # Get file options
                                    options = get_file_options(
                                        extracted_file.name,
                                        None if extension.lstrip('.').lower() != 'xlsx' else pd.ExcelFile(extracted_file).sheet_names,
                                        file.name,
                                    )

                                    file_options[extracted_file.name] = options
                                    # Load tables and register views in DuckDB
                                    loaded_tables = files_to_table(extracted_file, st.session_state.con, options, file.name)
                                    if loaded_tables:
                                        for table in loaded_tables:
                                            st.session_state.tables[table] = extracted_file.name
                                        loaded_tab += f"Loaded {file.name} - {extracted_file.name} as table(s): {', '.join(loaded_tables)}  \n"
                            else:
                                excluded_tab += f"{file.name} - {zip_info.filename} not loaded. Unsupported file format  \n"
            # Manage single files
            else:
                # Get file options
                options = get_file_options(file.name, None if file_extension != 'xlsx' else pd.ExcelFile(file).sheet_names)

                # Load tables and register views in DuckDB
                file_options[file.name] = options
                loaded_tables = files_to_table(file, st.session_state.con, options)
                if loaded_tables:
                    for table in loaded_tables:
                        st.session_state.tables[table] = file.name
                    loaded_tab += f"Loaded {file.name} as table(s): {', '.join(loaded_tables)}  \n"

    # Display success and warning messages
    if loaded_tab != "":
        st.success(loaded_tab)
    if excluded_tab != "":
        st.warning(excluded_tab)

    return st.session_state.tables


def get_file_options(file_name, sheet_names=None, archive_name=None):
    """
    Generates file options for CSV, TXT, and XLSX files based on user input.

    Args:
        file_name (str): The name of the file.
        sheet_names (list, optional): A list of sheet names for Excel files. Defaults to None.
        archive_name (str, optional): The name of the zip archive. Defaults to None.

    Returns:
        dict: A dictionary containing the selected options for the file or its sheets.

    Notes:
        - Displays Streamlit UI components for user input.
        - Supports options like headers, delimiters, and quoting for CSV/TXT files.
    """
    options = {}
    label_obj = f"{file_name}" if not archive_name else f"{archive_name} - {file_name}"
    file_extension = file_name.split('.')[-1].lower()
    if file_extension in ['csv', 'txt', 'xlsx']:
        with st.expander(f"File Settings - {label_obj}"):
            if file_extension == 'xlsx':
                left_column, right_column = st.columns(2)
                options['sheets'] = {}
                for index, sheet in enumerate(sheet_names):
                    with left_column if index % 2 == 0 else right_column:
                        options['sheets'][sheet] = {
                            'header': st.selectbox(
                                f"Header for {file_name} - {sheet}",
                                [0, None],
                                format_func=lambda x: "Yes" if x == 0 else "No",
                                key=f"header_{archive_name}_{file_name}_{sheet}",
                            )
                        }
            elif file_extension in ['csv', 'txt']:
                left_column, right_column = st.columns(2)
                with left_column:
                    options['header'] = st.selectbox(
                        f"Header",
                        [0, None],
                        format_func=lambda x: "Yes" if x == 0 else "No",
                        key=f"header_{archive_name}_{file_name}",
                    )
                    options['delimiter'] = st.text_input(f"Delimiter", ",", key=f"delimiter_{archive_name}_{file_name}")
                with right_column:
                    quoting_options = {
                        'QUOTE_ALL': csv.QUOTE_ALL,
                        'QUOTE_MINIMAL': csv.QUOTE_MINIMAL,
                        'QUOTE_NONNUMERIC': csv.QUOTE_NONNUMERIC,
                        'QUOTE_NONE': csv.QUOTE_NONE,
                    }
                    options['quoting'] = st.selectbox(
                        f"Quoting", list(quoting_options.keys()), key=f"quoting_{archive_name}_{file_name}"
                    )
                    options['quotechar'] = st.text_input(f"Quote character", '"', key=f"quote_{archive_name}_{file_name}")
                    options['dtype'] = "str"
    return options


def files_to_table(file, con, options=None, archive_name=None):
    """
    Get dataframe from file and register it in a database connection.

    Args:
        file: File object or path to the file.
        con: Database connection object.
        options: Dict of options for file loading.
        archive_name: Name of the zip archive.

    Returns:
        table_names(List): List of table names where the data was registered, or None if there was an error.
    """
    # Get file extension, file name, and archive name
    file_extension = file.name.split('.')[-1].lower()
    file_nm = f"{archive_name.replace('.', '_')}_{file.name.replace('.', '_')}" if archive_name else file.name.replace('.', '_')
    table_names = []

    try:
        # Manage CSV and TXT using collected file settings
        if file_extension in ['csv', 'txt']:
            delim = '\t' if options['delimiter'] == '\\t' else options['delimiter']
            # Read CSV using utf-8 encoding
            df = pd.read_csv(
                file,
                sep=delim,
                quoting=getattr(csv, options.get('quoting', 'QUOTE_NONE')),
                quotechar=options.get('quotechar', '"'),
                header=options.get('header', 0),
                dtype=options.get('dtype', str),
                on_bad_lines='skip',
                encoding_errors='ignore',
            )
            # In case of no header, rename columns from simple integer to col_integer
            if options.get('header', 0) is None:
                df.columns = [f'col_{i+1}' for i in range(len(df.columns))]
        # Manage XLSX using header settings collected
        elif file_extension == 'xlsx':
            xls = pd.ExcelFile(file)
            dfs = {}
            # Loop all sheets and in case of no header, rename columns from simple integer to col_integer
            for sheet_name in xls.sheet_names:
                sheet_options = options.get('sheets', {}).get(sheet_name, {})
                dfs[sheet_name] = pd.read_excel(file, sheet_name=sheet_name, header=sheet_options.get('header', 0))
                if sheet_options.get('header', 0) is None:
                    dfs[sheet_name].columns = [f'col_{i+1}' for i in range(len(dfs[sheet_name].columns))]
        # Manage other accepted file formats
        elif file_extension == 'parquet':
            df = pd.read_parquet(file)
        elif file_extension == 'avro':
            df = pdx.read_avro(file, na_dtypes=True)
        elif file_extension == 'json':
            json_data = json.load(file)
            if isinstance(json_data, dict):
                # It's a single object
                df = pd.DataFrame([json_data])
            else:
                # It's a list of objects
                df = pd.json_normalize(json_data)
        elif file_extension == 'xml':
            df = pd.read_xml(file)
        else:
            st.error(f"File {file.name} not loaded. Unsupported file format.")
            return None

        # Register dataframes into DuckDB
        if file_extension == 'xlsx':
            for sheet_name, df in dfs.items():
                sheet_name = sheet_name.lower()
                table_name = register_dataframe(con, df, f"{file_nm}_{sheet_name}")
                table_names.append(table_name)
        else:
            table_name = register_dataframe(con, df, file_nm)
            table_names.append(table_name)

        return table_names

    except Exception as e:
        st.error(f"Error loading file {file.name}: {str(e)}")
        # Manage exception of wrong file settings provided for CSV and TXT
        if file_extension in ['csv', 'txt'] and "Error tokenizing data" in str(e):
            st.warning(f'{file.name} not loaded. Please check file settings.')
        else:
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
        table_name(str): The name of the registered table.
    """
    # Clean table name
    table_name = clean_table_name(file_name)

    # Register view in db
    con.register(table_name, df)
    return table_name


def clean_table_name(name):
    """
    Cleans the table name by replacing spaces with underscores and removing all other special characters.
    Adds an underscore if the name starts or ends with a number.

    Args:
        name (str): The table name to be cleaned.

    Returns:
        name(str): The cleaned table name.
    """
    # Replace spaces with underscores
    name = name.replace(' ', '').lower()
    # Remove all other special characters
    name = sub(r'[^a-zA-Z0-9_]', '', name)

    # Add an underscore if the name starts with a number
    if name[0].isdigit():
        name = f"_{name}"
    # Add an underscore if the name ends with a number
    if name[-1].isdigit():
        name = f"{name}_"

    return name


def get_preview_data(con, table_name, num_rows=5):
    """
    Get preview data for a given table.

    Args:
        con (Connection): The DuckDB connection object.
        table_name (str): The name of the table to preview.
        num_rows (int): The number of rows to preview.

    Returns:
        df(pandas.DataFrame): A DataFrame containing the preview data.
    """
    # Get first 5 rows of the table
    query = f'SELECT * FROM "{table_name}" LIMIT {num_rows}'
    df = con.execute(query).fetchdf()
    # Reset index to start from 1
    df.index = range(1, len(df) + 1)
    return df


def data_preview(num_rows=5):
    """
    Display data preview for each table.

    Args:
        num_rows (int): The number of rows to preview.

    Returns:
        st.dataframe(preview_df): The preview of the selected table.
    """
    with st.expander("Data Preview", expanded=False):
        tab_prev = st.selectbox('Select Table:', st.session_state.tables.keys())
        preview_df = get_preview_data(st.session_state.con, tab_prev, num_rows)
        return st.dataframe(preview_df)


def get_query():
    """
    Function to get user input for SQL query and execute it, with table/column suggestions.

    Returns:
        st.session_state.query_result(pd.DataFrame): The result of the executed query, or None if no query was entered.
    """
    st.subheader("Query Data")
    st.caption("Type your SQL query below and press \"Ctrl + Enter\" to run it.")
    # Define SQL completions based on loaded tables and columns
    sql_completions()
    
    # Define action to submit written query. Ctrl + Enter or mouse click
    query_btn = [{
        "name": "Run",
        "feather": "Play",
        "primary": True,
        "hasText": True,
        "showWithIcon": True,
        "commands": ["saveState","submit"],
        "style": {"bottom": "0.44rem", "right": "0.4rem"},
        "alwaysOn": True
    }]

    # Define code editor options
    sql_query_input = code_editor(
        code=st.session_state.query_statement,
        lang='sql',
        shortcuts="vscode",
        options={"enableBasicAutocompletion": True, 
                 "enableLiveAutocompletion": True, 
                 "showLineNumbers":True,
                 "highlightActiveLine": True,
                 "highlightSelectedWord":True},
        height=[5, 7],
        buttons=query_btn,
        focus=True,
        completions=st.session_state.completions,
        key=f'sql_query_{len(st.session_state.completions)}',
        allow_reset=False
    )

    # Get SQL query from code editor once sumitted
    if sql_query_input["text"] != st.session_state.query_statement and sql_query_input["text"] != "":
        st.session_state.query_statement = sql_query_input["text"]

    # Button to run query
    #if st.button("Run Query"):
    if st.session_state.query_statement.strip() != "":
        try:
            # Run query
            st.session_state.query_result = None
            st.session_state.edited_df = None
            result_df = run_query(st.session_state.con, st.session_state.query_statement)
            # Reset index to start from 1 for query results
            result_df.index = range(1, len(result_df) + 1)
            st.session_state.query_result = result_df
            #st.success("Query executed successfully!")
        # Catch exception of wrong table name and update command
        except Exception as e:
            if "Catalog Error: Table with name" in str(e):
                st.error("Table not existing. Please check table names in your query.")
            elif "Can only update base table" in str(e):
                st.error("Update not available. Please consider a different select statement and the edit mode.")
            else:
                st.error(f"Error executing query: {str(e)}")
    else:
        st.session_state.query_result = None
    #:
    #    st.warning("Please enter a SQL query.")


def run_query(con, sql_query):
    """
    Executes the given SQL query on the provided connection object and returns the result.

    Args:
        con (connection): The connection object to the database.
        sql_query (str): The SQL query to be executed.

    Returns:
        result(pd.DataFrame): The result of the SQL query as a DataFrame.
    """
    # Execute SQL query
    result = con.execute(sql_query).fetchdf()
    return result


def query_result():
    """
    Displays the query result and allows for editing and exporting of data.

    Returns:
        st.session_state.query_result, st.session_state.export_df(Tuple): A tuple containing the query result dataframe and the export dataframe.
    """
    st.subheader("Query Result")
    # Add a toggle for edit mode
    edit_mode = st.toggle("Edit Mode")
    # Manage the edit mode
    if edit_mode:
        st.session_state.edited_df = st.data_editor(st.session_state.query_result, num_rows="dynamic")
    else:
        if st.session_state.edited_df is not None:
            st.session_state.query_result = st.session_state.edited_df.copy()
        st.dataframe(st.session_state.query_result)

    # Prepare dataframe for export data
    if st.session_state.edited_df is not None:
        st.session_state.export_df = st.session_state.edited_df.copy()
    else:
        st.session_state.export_df = st.session_state.query_result.copy()

    return st.session_state.query_result, st.session_state.export_df


def data_download(file_ext):
    """
    Function to download data in different file formats.

    Parameters:
        file_ext (List[str]): List of file extensions available for download.

    Returns:
        st.download_button: Download button component that allows the user to download the selected file format.
    """
    col1, col2 = st.columns(2)
    with col1:
        with st.popover("Data Download"):
            # File format selection
            file_formats = [item for item in file_ext if item != 'zip']
            selected_format = st.selectbox("Select file format:", file_formats)

            # Delimiter and quoting options (for CSV)
            if selected_format in ['csv', 'txt']:
                header = st.selectbox("Header:", ("Y", "N"))
                delimiter = st.text_input("Delimiter:", max_chars=1, value=",")
                quoting_options = {
                    'QUOTE_ALL': csv.QUOTE_ALL,
                    'QUOTE_MINIMAL': csv.QUOTE_MINIMAL,
                    'QUOTE_NONNUMERIC': csv.QUOTE_NONNUMERIC,
                    'QUOTE_NONE': csv.QUOTE_NONE
                }
                quoting = st.selectbox("Quoting:", list(quoting_options.keys()))
                head = True if header == 'Y' else False

                file_content = df_to_file(
                    st.session_state.export_df, selected_format,
                    sep=delimiter, quoting=quoting_options[quoting], header=head
                )
            # Manage XLSX download
            elif selected_format == 'xlsx':
                header = st.selectbox("Header:", ("Y", "N"))
                head = True if header == 'Y' else False
                file_content = df_to_file(st.session_state.export_df, selected_format, header=head)
            # Manage other file download
            else:
                file_content = df_to_file(st.session_state.export_df, selected_format)

            # Generate file name and MIME type
            file_name = f"query_result.{selected_format}"
            mime_types = {
                'csv': 'text/csv',
                'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'json': 'application/json',
                'parquet': 'application/octet-stream',
                'xml': 'application/xml'
            }
            mime_type = mime_types.get(selected_format, 'application/octet-stream')

            # Download button
            return st.download_button(
                label=f"Download",
                data=file_content,
                file_name=file_name,
                mime=mime_type,
            )


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
    # Create the buffer
    buffer = BytesIO()

    try:
        # Manage CSV and TXT files
        if file_format in ['csv', 'txt']:
            #kwargs['dtype'] = str
            try:
                df.to_csv(buffer, index=False, **kwargs)
            # Catch specific CSV writing errors
            except csv.Error as e:
                if "need to escape" in str(e):
                    st.warning("Special character found in the data.  \nPlease select a different quoting option.")
                else:
                    raise ValueError(f"{file_format} writing error: {e}")
        # Manage XLSX files
        elif file_format == 'xlsx':
            df.to_excel(buffer, index=False, engine='openpyxl', **kwargs)
        # Manage JSON files
        elif file_format == 'json':
            df.to_json(buffer, orient='records', **kwargs)
        # Manage Parquet files
        elif file_format == 'parquet':
            df.to_parquet(buffer, index=False, engine='pyarrow', **kwargs)
        # Manage XML files
        elif file_format == 'xml':
            df.to_xml(buffer, index=False, **kwargs)
        # Manage Avro files
        elif file_format == 'avro':
            pdx.to_avro(buffer, df)
        # Get unsupported files error
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    # Catch errors for df to file conversion
    except Exception as e:
        st.error(e)
        st.warning(f"{file_format} export not available for your data.  \nPlease select a different format.")

    # Return the buffer
    buffer.seek(0)
    return buffer.getvalue()


def sql_completions():
    """
    Function to generate SQL completions for the code editor.

    Returns:
        None
    """
    try:
        # Get connection
        con = st.session_state.con
        # Get loaded tables list
        tab_list = st.session_state.tables
        # Reset completions list
        st.session_state.completions = []

        # Loop all tables and get columns names
        for table_name in tab_list:
            # Ensure table_name is a non-empty string before proceeding
            if isinstance(table_name, str) and table_name:
                # Add table name completion
                st.session_state.completions.append({
                    "caption": table_name,
                    "value": f'"{table_name}"',  # Add quotes for safety
                    "meta": "table",
                    "name": f"table_{table_name}",  # Added unique name
                    "score": 200
                })

                # Get column names for the table
                try:
                    # Use PRAGMA table_info to fetch column metadata
                    columns_df = con.execute(f'PRAGMA table_info("{table_name}")').fetchdf()
                    if not columns_df.empty and 'name' in columns_df.columns:
                        for column_name in columns_df['name']:
                            # Ensure column_name is a non-empty string
                            if isinstance(column_name, str) and column_name:
                                # Add column name completion
                                st.session_state.completions.append({
                                    "caption": column_name,
                                    "value": f'"{column_name}"',  # Add quotes for safety
                                    "meta": "column",
                                    "name": f"col_{table_name}_{column_name}",  # Added unique name
                                    "score": 100
                                })
                except Exception as e:
                    st.warning(f"Could not fetch columns for table '{table_name}': {e}")
                    pass
    except Exception as e:
        st.warning(f"Could not generate SQL suggestions: {e}")
        pass


def main():
    """
    The main function of the DataQuery application.

    This function handles the main logic of the application, including file uploading, data loading, data preview, SQL query execution, and result visualization.
    It collects files settings for csv, txt and xlsx files.
    It also provides options for editing and downloading query results.

    Returns:
        None
    """
    # Set page config
    st.set_page_config(page_title='DataQuery', page_icon=':o:', layout="wide")
    # Set title and subheader
    st.title("DataQuery")
    st.subheader("Preview, query, edit and export data files")

    # Supported file extensions
    file_ext = ['avro', 'csv', 'json', 'parquet', 'txt', 'xlsx', 'xml', 'zip']
    # Number of rows read for preview
    num_rows = 5

    # Initialize session state variables
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
        st.session_state.export_df = None
    if 'completions' not in st.session_state:
        st.session_state.completions = []
    if 'query_statement' not in st.session_state:
        st.session_state.query_statement = ''

    # Files upload
    upload_files(file_ext)

    # If files uploaded
    if st.session_state.uploaded_files:
        # Load files to db
        files_to_db(file_ext)

        # If tables created
        if st.session_state.tables:
            # Display data preview for each table
            data_preview(num_rows=num_rows)
            # Provide SQL query section
            get_query()

        # If query result
        if st.session_state.query_result is not None:
            # Display query result
            query_result()

            # Display data download section
            data_download(file_ext)
    else:
        # Reset session state variables
        st.session_state.tables = {}
        st.session_state.query_result = None
        st.session_state.export_df = None
        st.session_state.uploaded_files = []
        st.session_state.edited_df = None
        st.session_state.completions = []
        st.session_state.query_statement = ''


if __name__ == "__main__":
    main()