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
    new_tables_list = []

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
                                    # Load tables and register views in DuckDB (if enabled)
                                    if options.get('load', True):
                                        loaded_tables = files_to_table(extracted_file, st.session_state.con, options, file.name)
                                        if loaded_tables:
                                            for table in loaded_tables:
                                                st.session_state.tables[table] = extracted_file.name
                                                new_tables_list.append(table)
                                            tables_list = ''.join([f'  \n- {t}' for t in loaded_tables])
                                            loaded_tab += f"Loaded {file.name} - {extracted_file.name} as table(s):{tables_list}  \n\n"
                            else:
                                excluded_tab += f"{file.name} - {zip_info.filename} not loaded. Unsupported file format  \n"
            # Manage single files
            else:
                # Get file options
                options = get_file_options(file.name, None if file_extension != 'xlsx' else pd.ExcelFile(file).sheet_names)

                # Load tables and register views in DuckDB (if enabled)
                file_options[file.name] = options
                if options.get('load', True):
                    loaded_tables = files_to_table(file, st.session_state.con, options)
                    if loaded_tables:
                        for table in loaded_tables:
                            st.session_state.tables[table] = file.name
                            new_tables_list.append(table)
                        tables_list = ''.join([f'  \n- {t}' for t in loaded_tables])
                        loaded_tab += f"Loaded {file.name} as table(s):{tables_list}  \n\n"

    # Cleanup obsolete tables (renamed aliases or unselected sheets)
    saved_tables = st.session_state.get('saved_tables', set())
    tables_to_remove = [table for table in list(st.session_state.tables.keys())
                        if table not in new_tables_list and table not in saved_tables]
    for table in tables_to_remove:
        remove_view(st.session_state.con, table)
        del st.session_state.tables[table]

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
    
    with st.expander(f"File Settings - {label_obj}"):
        # Load toggle shown first, on its own row (default: yes)
        options['load'] = st.checkbox(
            "Load this file",
            value=True,
            key=f"load_{archive_name}_{file_name}",
        )
        if file_extension == 'xlsx':
            selected_sheets = st.multiselect(
                f"Sheets to load from {file_name}",
                sheet_names,
                default=sheet_names,
                key=f"sheets_{archive_name}_{file_name}",
            )
            options['selected_sheets'] = selected_sheets
            left_column, right_column = st.columns(2)
            options['sheets'] = {}
            for index, sheet in enumerate(selected_sheets):
                with left_column if index % 2 == 0 else right_column:
                    options['sheets'][sheet] = {
                        'header': st.selectbox(
                            f"Header for {file_name} - {sheet}",
                            [0, None],
                            format_func=lambda x: "Yes" if x == 0 else "No",
                            key=f"header_{archive_name}_{file_name}_{sheet}",
                        ),
                        'alias': st.text_input(
                            f"Table alias for {file_name} - {sheet}",
                            value="",
                            key=f"alias_{archive_name}_{file_name}_{sheet}",
                            help="Leave empty to use default name",
                        ),
                    }
        else:
            if file_extension in ['csv', 'txt']:
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
            
            options['alias'] = st.text_input(
                f"Table alias for {file_name}",
                value="",
                key=f"alias_{archive_name}_{file_name}",
                help="Leave empty to use default name",
            )
            
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
            selected_sheets = options.get('selected_sheets', xls.sheet_names)
            # Loop selected sheets and in case of no header, rename columns from simple integer to col_integer
            for sheet_name in xls.sheet_names:
                if sheet_name not in selected_sheets:
                    continue
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
            used_names = {}
            for sheet_name, df in dfs.items():
                sheet_options = options.get('sheets', {}).get(sheet_name, {})
                alias = sheet_options.get('alias', '').strip()
                if alias:
                    resolved_name = clean_table_name(alias)
                else:
                    resolved_name = clean_table_name(f"{file_nm}_{sheet_name.lower()}")
                # Check for duplicate table names
                if resolved_name in used_names:
                    st.error(f"Duplicate alias: sheet \"{sheet_name}\" resolves to table \"{resolved_name}\", "
                             f"already used by sheet \"{used_names[resolved_name]}\". Skipping.")
                    continue
                used_names[resolved_name] = sheet_name
                table_name = register_dataframe(con, df, resolved_name)
                table_names.append(table_name)
        else:
            alias = options.get('alias', '').strip() if options else ''
            if alias:
                resolved_name = clean_table_name(alias)
            else:
                resolved_name = clean_table_name(file_nm)
                
            table_name = register_dataframe(con, df, resolved_name)
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


def build_session_zip():
    """
    Builds a ZIP archive containing one parquet file per session table.

    The archive is built entirely in memory (BytesIO): no data is ever written
    to the server filesystem, respecting the application's data policy.
    Each table is exported as "<table_name>.parquet" so that re-uploading the
    ZIP restores the session with the same table names.

    Returns:
        bytes: The ZIP archive as bytes, or None if it could not be built.
    """
    con = st.session_state.con
    zip_buffer = BytesIO()

    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            # Write each session table as a parquet file
            for table_name in st.session_state.tables:
                try:
                    df = con.execute(f'SELECT * FROM "{table_name}"').fetchdf()
                except Exception as e:
                    st.warning(f"Could not export table '{table_name}': {e}")
                    continue
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                zf.writestr(f"{table_name}.parquet", parquet_buffer.getvalue())
    except Exception as e:
        st.error(f"Could not build session file: {e}")
        return None

    return zip_buffer.getvalue()


def session_export():
    """
    Displays a popover to save the current session as an in-memory ZIP of parquet files.

    The bundle includes every table currently in the session (loaded files and any
    tables saved from query results) and is generated in RAM when the popover is
    opened; nothing is persisted on the server.

    Returns:
        None
    """
    with st.popover("Save Session"):
        st.caption(
            "Save all session tables as a ZIP of parquet files.  \n"
            "Re-upload the ZIP to restore the session."
        )

        # Build the ZIP bundle in memory (on popover open)
        zip_bytes = build_session_zip()

        if zip_bytes:
            st.download_button(
                label="Download",
                data=zip_bytes,
                file_name="dataquery_session.zip",
                mime="application/zip",
            )


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

    import hashlib
    comp_hash = hashlib.md5(str(st.session_state.completions).encode()).hexdigest()

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
        key=f'sql_query_{comp_hash}',
        allow_reset=False
    )

    # Get SQL query from code editor once sumitted
    if sql_query_input["text"] != st.session_state.query_statement:
        st.session_state.query_statement = sql_query_input["text"]

        # Button to run query
        #if st.button("Run Query"):
        if st.session_state.query_statement.strip() != "":
            try:
                # Run query
                st.session_state.query_result = None
                st.session_state.edited_df = None
                result_df = run_query(st.session_state.con, st.session_state.query_statement)
                #check if query got result
                if result_df is not None:
                    # Reset index to start from 1 for query results (empty range if no rows)
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
    try:
        result = con.execute(sql_query).fetchdf()
    except AttributeError as e:
        return None
    return result


def query_result():
    """
    Displays the query result and allows for editing and exporting of data.

    Returns:
        st.session_state.query_result, st.session_state.export_df(Tuple): A tuple containing the query result dataframe and the export dataframe.
    """
    st.subheader("Query Result")
    # Notify when the query ran successfully but returned no rows
    if st.session_state.query_result is not None and st.session_state.query_result.empty:
        st.info("There is no data to display.")
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
    col1, col2, _ = st.columns([1, 1, 5])
    with col1:
        # Provide save of the current query result as a new session table
        save_as_table()
    with col2:
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
            st.download_button(
                label=f"Download",
                data=file_content,
                file_name=file_name,
                mime=mime_type,
            )

    


def save_as_table():
    """
    Saves the current query result (export_df) as a new named table in the session.

    The table is registered as a view in DuckDB and tracked in
    st.session_state.saved_tables so it survives reruns (it is excluded from the
    file-cleanup logic) and is included in the session export.

    Returns:
        None
    """
    with st.popover("Save as Table"):
        st.caption("Save the current query result as a new session table.")
        new_name = st.text_input(
            "Table name",
            value="",
            key="save_as_table_name",
            help="Name for the new table created from the current query result",
        )
        if st.button("Save", key="save_as_table_btn"):
            if st.session_state.get('export_df') is None:
                st.warning("No query result to save.")
                return
            resolved_name = clean_table_name(new_name)
            if not resolved_name:
                st.warning("Please provide a valid table name.")
                return
            if resolved_name in st.session_state.tables:
                st.error(f"Table '{resolved_name}' already exists. Choose a different name.")
                return
            # Register a snapshot of the current result as a view in DuckDB
            register_dataframe(st.session_state.con, st.session_state.export_df.copy(), resolved_name)
            st.session_state.tables[resolved_name] = "(saved table)"
            st.session_state.saved_tables.add(resolved_name)
            # Refresh the app so the new table appears everywhere
            st.rerun()


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
    st.set_page_config(page_title='DataQuery', page_icon=":material/table:", layout="wide")
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
    if 'saved_tables' not in st.session_state:
        st.session_state.saved_tables = set()

    # Files upload
    upload_files(file_ext)

    # If files uploaded
    if st.session_state.uploaded_files:
        # Load files to db
        files_to_db(file_ext)

        # If tables created
        if st.session_state.tables:
            # Persistently show tables saved from query results (like the loaded message)
            saved_tabs = [t for t in st.session_state.tables if t in st.session_state.saved_tables]
            if saved_tabs:
                saved_list = ''.join([f'  \n- {t}' for t in saved_tabs])
                st.success(f"Saved query result as table(s):{saved_list}")
            # Display data preview for each table
            data_preview(num_rows=num_rows)
            # Provide session save (all tables, optional query result) as in-memory ZIP
            session_export()
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
        st.session_state.saved_tables = set()


if __name__ == "__main__":
    main()