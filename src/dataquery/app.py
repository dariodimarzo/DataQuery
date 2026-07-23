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
        - Removes tables from the database for files that are no longer uploaded.
    """
    # Upload files object
    uploaded_files = st.file_uploader(
        "Choose data files",
        accept_multiple_files=True,
        help='Upload your data files and zip archives.  \nAll files from zip archives and all sheets of xlsx files will be considered.',
        type=file_ext,
        key=f"uploader_{st.session_state.get('uploader_key', 0)}",
    )

    # Check for removed files
    removed_files = [file for file in st.session_state.uploaded_files if file not in uploaded_files]
    # Remove tables from the database for removed files
    for file in removed_files:
        tables_to_remove = [table for table, source in st.session_state.tables.items() if source == file.name]
        for table in tables_to_remove:
            remove_table(st.session_state.con, table)
            del st.session_state.tables[table]
            st.session_state.get('table_signatures', {}).pop(table, None)
        st.warning(f"Removed file: {file.name} and its associated tables")

    # Update the list of uploaded files
    st.session_state.uploaded_files = uploaded_files

    return st.session_state.uploaded_files


def remove_table(con, table_name):
    """
    Safely removes a table from the DuckDB connection.

    Args:
        con (duckdb.Connection): The DuckDB connection object.
        table_name (str): The name of the table to remove.

    Returns:
        None

    Notes:
        - If the table does not exist or cannot be dropped, a warning is displayed.
    """
    try:
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    except duckdb.CatalogException as e:
        st.warning(f"Could not drop table {table_name}: {str(e)}")


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
    # Track names already in use (saved tables, SQL-created objects and names
    # resolved during this run) so default names can be disambiguated across
    # files and sheets.
    existing_names = set(st.session_state.get('saved_tables', set())) | set(st.session_state.get('sql_objects', set()))

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
                                    # Load tables and materialize them in DuckDB (if enabled)
                                    if options.get('load', True):
                                        loaded_tables = files_to_table(extracted_file, st.session_state.con, options, file.name, existing_names)
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

                # Load tables and materialize them in DuckDB (if enabled)
                file_options[file.name] = options
                if options.get('load', True):
                    loaded_tables = files_to_table(file, st.session_state.con, options, existing_names=existing_names)
                    if loaded_tables:
                        for table in loaded_tables:
                            st.session_state.tables[table] = file.name
                            new_tables_list.append(table)
                        tables_list = ''.join([f'  \n- {t}' for t in loaded_tables])
                        loaded_tab += f"Loaded {file.name} as table(s):{tables_list}  \n\n"

    # Cleanup obsolete tables (renamed aliases or unselected sheets)
    saved_tables = st.session_state.get('saved_tables', set())
    sql_objects = st.session_state.get('sql_objects', set())
    tables_to_remove = [table for table in list(st.session_state.tables.keys())
                        if table not in new_tables_list and table not in saved_tables
                        and table not in sql_objects]
    for table in tables_to_remove:
        remove_table(st.session_state.con, table)
        del st.session_state.tables[table]
        st.session_state.get('table_signatures', {}).pop(table, None)

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
                "Sheets to load",
                sheet_names,
                default=sheet_names,
                key=f"sheets_{archive_name}_{file_name}",
            )
            options['selected_sheets'] = selected_sheets
            options['sheets'] = {}
            # One clear settings row per sheet: Header and Table alias side by side
            for sheet in selected_sheets:
                header_col, alias_col = st.columns(2)
                with header_col:
                    sheet_header = st.selectbox(
                        f"Header - {sheet}",
                        [0, None],
                        format_func=lambda x: "Yes" if x == 0 else "No",
                        key=f"header_{archive_name}_{file_name}_{sheet}",
                    )
                with alias_col:
                    sheet_alias = st.text_input(
                        f"Table alias - {sheet}",
                        value="",
                        key=f"alias_{archive_name}_{file_name}_{sheet}",
                        help="Leave empty to use default name",
                    )
                options['sheets'][sheet] = {'header': sheet_header, 'alias': sheet_alias}
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

            alias_col,_ = st.columns(2)
            with alias_col:
                options['alias'] = st.text_input(
                    "Table alias",
                    value="",
                    key=f"alias_{archive_name}_{file_name}",
                    help="Leave empty to use default name"
                )
            
    return options


def files_to_table(file, con, options=None, archive_name=None, existing_names=None):
    """
    Get dataframe from file and register it in a database connection.

    Args:
        file: File object or path to the file.
        con: Database connection object.
        options: Dict of options for file loading.
        archive_name: Name of the zip archive.
        existing_names: Set of table names already in use, used to avoid
            collisions when generating default names. Mutated in place with the
            names resolved for this file.

    Returns:
        table_names(List): List of table names where the data was registered, or None if there was an error.
    """
    # Get file extension and base names (without extension) used for default naming
    file_extension = file.name.split('.')[-1].lower()
    file_base = splitext(file.name)[0]
    archive_base = splitext(archive_name)[0] if archive_name else None
    if existing_names is None:
        existing_names = set()
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
            single_sheet = len(dfs) == 1
            for sheet_name, df in dfs.items():
                sheet_options = options.get('sheets', {}).get(sheet_name, {})
                alias = sheet_options.get('alias', '').strip()
                if alias:
                    # Explicit alias: honour it as-is, but avoid silent collisions
                    resolved_name = clean_table_name(alias)
                    if not resolved_name:
                        st.error(f"Invalid alias for sheet \"{sheet_name}\". Skipping.")
                        continue
                    if resolved_name in existing_names:
                        st.error(f"Duplicate alias: sheet \"{sheet_name}\" resolves to table "
                                 f"\"{resolved_name}\", already in use. Skipping.")
                        continue
                else:
                    # Default name: shortest readable option, qualify only on conflict
                    if single_sheet:
                        candidates = [file_base]
                        if archive_base:
                            candidates.append(f"{archive_base}_{file_base}")
                    else:
                        candidates = [sheet_name, f"{file_base}_{sheet_name}"]
                        if archive_base:
                            candidates.append(f"{archive_base}_{file_base}_{sheet_name}")
                    resolved_name = resolve_unique_name(candidates, existing_names)
                existing_names.add(resolved_name)
                signature = table_signature(file, options, archive_name, sheet_name)
                table_name = materialize_table(con, df, resolved_name, signature)
                table_names.append(table_name)
        else:
            alias = options.get('alias', '').strip() if options else ''
            if alias:
                # Explicit alias: honour it as-is, but avoid silent collisions
                resolved_name = clean_table_name(alias)
                if not resolved_name:
                    st.error(f"Invalid alias for {file.name}. Skipping.")
                    return None
                if resolved_name in existing_names:
                    st.error(f"Duplicate alias: {file.name} resolves to table "
                             f"\"{resolved_name}\", already in use. Skipping.")
                    return None
            else:
                # Default name: file name without extension, qualify only on conflict
                candidates = [file_base]
                if archive_base:
                    candidates.append(f"{archive_base}_{file_base}")
                resolved_name = resolve_unique_name(candidates, existing_names)
            existing_names.add(resolved_name)
            signature = table_signature(file, options, archive_name)
            table_name = materialize_table(con, df, resolved_name, signature)
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
    Materialize a DataFrame as a writable base table in the connection.

    The DataFrame is first exposed through a temporary view and then copied into
    a real DuckDB table with CREATE TABLE ... AS SELECT.

    Args:
        con (Connection): The connection object to register the DataFrame.
        df (pd.DataFrame): The DataFrame to register.
        file_name (str): The name of the file (used to generate the table name).

    Returns:
        table_name(str): The name of the created table.
    """
    # Clean table name
    table_name = clean_table_name(file_name)

    # Expose the DataFrame through a temporary view, then materialize it as a base table
    tmp_view = f"_src_{table_name}"
    con.register(tmp_view, df)
    try:
        con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM "{tmp_view}"')
    finally:
        con.unregister(tmp_view)

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


def resolve_unique_name(candidates, existing_names):
    """
    Resolves a unique table name from an ordered list of candidate names.

    Candidates must be ordered from the simplest/most readable to the most
    qualified (e.g. sheet name, then file_sheet, then archive_file_sheet). The
    first candidate that does not collide with an already-used name is returned.
    If every candidate collides, a numeric suffix is appended to the most
    qualified candidate until a free name is found.

    Args:
        candidates (list): Ordered candidate names (raw, not yet cleaned).
        existing_names (set): Names already in use that must be avoided.

    Returns:
        str: A cleaned, unique table name.
    """
    cleaned_candidates = [c for c in (clean_table_name(x) for x in candidates) if c]
    if not cleaned_candidates:
        cleaned_candidates = ['table']
    # Prefer the first candidate that is not already taken
    for name in cleaned_candidates:
        if name not in existing_names:
            return name
    # Every candidate collided: fall back to a numeric suffix on the most qualified one
    base = cleaned_candidates[-1]
    i = 2
    while f"{base}_{i}" in existing_names:
        i += 1
    return f"{base}_{i}"


def table_exists(con, name):
    """
    Checks whether a base table with the given name exists in the connection.

    Args:
        con (Connection): The DuckDB connection object.
        name (str): The table name to look up.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        row = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()
        return row is not None
    except Exception:
        return False


def table_signature(file, options, archive_name=None, sheet_name=None):
    """
    Builds a signature describing a source file and its load options.

    The signature is used to decide whether a table must be rebuilt on rerun. It
    combines the file identity (name, archive, size) with the options that affect
    the resulting data, so that changing any load setting triggers a rebuild.

    Args:
        file: The uploaded file object.
        options (dict): The load options collected for the file.
        archive_name (str, optional): The zip archive name, if any.
        sheet_name (str, optional): The sheet name for xlsx files.

    Returns:
        str: A stable JSON signature string.
    """
    size = getattr(file, 'size', None)
    if size is None:
        try:
            size = len(file.getbuffer())
        except Exception:
            size = None
    payload = {'file': file.name, 'archive': archive_name, 'size': size}
    if sheet_name is not None:
        sheet_opts = (options or {}).get('sheets', {}).get(sheet_name, {})
        payload.update({
            'sheet': sheet_name,
            'header': sheet_opts.get('header', 0),
            'alias': sheet_opts.get('alias', ''),
        })
    else:
        opts = options or {}
        payload.update({
            'header': opts.get('header', 0),
            'delimiter': opts.get('delimiter'),
            'quoting': opts.get('quoting'),
            'quotechar': opts.get('quotechar'),
            'alias': opts.get('alias', ''),
        })
    return json.dumps(payload, sort_keys=True, default=str)


def materialize_table(con, df, table_name, signature):
    """
    Creates or preserves a base table for a loaded DataFrame.

    If a table with the same name and an identical load signature already exists,
    it is kept as-is otherwise the table is (re)created from the DataFrame 
    to reflect new content or settings.

    Args:
        con (Connection): The DuckDB connection object.
        df (pd.DataFrame): The DataFrame to materialize.
        table_name (str): The resolved table name.
        signature (str): Signature describing the source file and its load options.

    Returns:
        str: The table name.
    """
    signatures = st.session_state.setdefault('table_signatures', {})
    # Preserve an existing table when nothing relevant changed
    if signatures.get(table_name) == signature and table_exists(con, table_name):
        return table_name
    register_dataframe(con, df, table_name)
    signatures[table_name] = signature
    return table_name


def sync_catalog(con):
    """
    Reconciles the app table catalog with the actual objects in DuckDB.

    Reads base tables and views from DuckDB (schema 'main') and updates
    st.session_state.tables so that objects created via SQL (CREATE TABLE/VIEW)
    appear in the UI and objects dropped via SQL (DROP TABLE/VIEW) disappear from
    it. Internal temporary views (prefixed with '_src_') are ignored. Objects
    created via SQL are tracked in st.session_state.sql_objects so the file
    loading cleanup does not remove them.

    Args:
        con (Connection): The DuckDB connection object.

    Returns:
        bool: True if the catalog changed, False otherwise.
    """
    st.session_state.setdefault('sql_objects', set())
    try:
        rows = con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    except Exception:
        return False

    live = {name for (name,) in rows if not name.startswith('_src_')}
    changed = False

    # Add SQL-created objects not yet tracked by the app
    for name in live:
        if name not in st.session_state.tables:
            st.session_state.tables[name] = "(sql)"
            st.session_state.sql_objects.add(name)
            changed = True

    # Drop from the catalog objects that no longer exist in DuckDB
    for name in list(st.session_state.tables.keys()):
        if name not in live:
            st.session_state.tables.pop(name, None)
            st.session_state.saved_tables.discard(name)
            st.session_state.sql_objects.discard(name)
            st.session_state.get('table_signatures', {}).pop(name, None)
            changed = True

    return changed


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
        dataprev_col,_ = st.columns(2)
        with dataprev_col:
            tab_prev = st.selectbox('Select Table:', st.session_state.tables.keys())
        preview_df = get_preview_data(st.session_state.con, tab_prev, num_rows)
        return st.dataframe(preview_df)


def build_session_zip():
    """
    Builds a ZIP archive containing one parquet file per session table.

    The archive is built entirely in memory (BytesIO): no data is ever written
    to the server filesystem.
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
    with st.popover("Save Session", use_container_width=True):
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


def new_session():
    """
    Resets the whole session to a clean state.

    Recreates the in-memory DuckDB connection (which drops every table and view
    at once), clears all app catalogs and query state, and empties the file
    uploader by bumping its dynamic key. Triggers a rerun so the UI refreshes.

    Returns:
        None
    """
    # Recreate the connection: this drops all tables/views held in memory
    st.session_state.con = duckdb.connect(database=':memory:')
    # Clear every catalog and query-related state
    st.session_state.tables = {}
    st.session_state.query_result = None
    st.session_state.export_df = None
    st.session_state.edited_df = None
    st.session_state.completions = []
    st.session_state.query_statement = ''
    st.session_state.saved_tables = set()
    st.session_state.table_signatures = {}
    st.session_state.sql_objects = set()
    st.session_state.uploaded_files = []
    # Bump the uploader key so the file_uploader widget is emptied
    st.session_state.uploader_key = st.session_state.get('uploader_key', 0) + 1
    st.rerun()


def session_controls():
    """
    Renders the session action buttons (New Session and Save Session) on one row.

    - New Session drops every table/view and clears the session for a clean start.
    - Save Session downloads all session tables as an in-memory ZIP of parquet files.

    Returns:
        None
    """
    col_new, col_save, _ = st.columns([1, 1, 5])
    with col_new:
        if st.button("New Session", help="Remove all objects and start from a clean session", use_container_width=True):
            new_session()
    with col_save:
        session_export()


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

    # Get SQL query from code editor once sumitted (only on an actual submit event,
    # so the editor remounting on completions change does not wipe the result)
    if sql_query_input.get("type") == "submit" and sql_query_input["text"] != st.session_state.query_statement:
        st.session_state.query_statement = sql_query_input["text"]
        catalog_changed = False

        # Button to run query
        #if st.button("Run Query"):
        if st.session_state.query_statement.strip() != "":
            try:
                # Run query
                st.session_state.query_result = None
                st.session_state.edited_df = None
                result_df = run_query(st.session_state.con, st.session_state.query_statement)
                # Reflect any CREATE/DROP TABLE/VIEW issued via SQL in the UI catalog
                catalog_changed = sync_catalog(st.session_state.con)
                #check if query got result
                if result_df is not None:
                    # Reset index to start from 1 for query results (empty range if no rows)
                    result_df.index = range(1, len(result_df) + 1)
                    st.session_state.query_result = result_df
                    #st.success("Query executed successfully!")
            # Catch exception of wrong table name and update command
            except Exception as e:
                err = str(e)
                if "already exists" in err:
                    st.error("Table/View already existing. Please choose a different name or use CREATE OR REPLACE.")
                elif "Catalog Error: Table with name" in err and "does not exist" in err:
                    st.error("Table not existing. Please check table names in your query.")
                elif "Can only update base table" in err:
                    st.error("Update not available. Please consider a different select statement and the edit mode.")
                else:
                    st.error(f"Error executing query: {str(e)}")
        else:
            st.session_state.query_result = None

        # Refresh the UI (preview, autocompletion, ...) when the catalog changed
        if catalog_changed:
            st.rerun()
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
        with st.popover("Data Download", use_container_width=True):
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

    The table is materialized as a base table in DuckDB and tracked in
    st.session_state.saved_tables so it survives reruns (it is excluded from the
    file-cleanup logic) and is included in the session export.

    Returns:
        None
    """
    with st.popover("Save as Table", use_container_width=True):
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
            # Materialize a snapshot of the current result as a base table in DuckDB
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
    if 'table_signatures' not in st.session_state:
        st.session_state.table_signatures = {}
    if 'sql_objects' not in st.session_state:
        st.session_state.sql_objects = set()
    if 'uploader_key' not in st.session_state:
        st.session_state.uploader_key = 0

    # Files upload
    upload_files(file_ext)

    # Reconcile the database with the current files: this loads newly uploaded
    # files and, thanks to its cleanup step, drops tables of removed files even
    # when no file is left (saved/SQL tables are preserved).
    files_to_db(file_ext)

    # Show the working UI whenever there is at least one table (loaded from files,
    # saved from results, or created via SQL). Use "New Session" for a clean start.
    if st.session_state.tables:
        # Persistently show tables saved from query results (like the loaded message)
        saved_tabs = [t for t in st.session_state.tables if t in st.session_state.saved_tables]
        if saved_tabs:
            saved_list = ''.join([f'  \n- {t}' for t in saved_tabs])
            st.success(f"Saved query result as table(s):{saved_list}")
        # Persistently show tables/views created via SQL
        sql_tabs = [t for t in st.session_state.tables if t in st.session_state.sql_objects]
        if sql_tabs:
            sql_list = ''.join([f'  \n- {t}' for t in sql_tabs])
            st.success(f"Created table(s) via SQL:{sql_list}")
        # Display data preview for each table
        data_preview(num_rows=num_rows)
        # Session controls: New Session + Save Session on the same row
        session_controls()
        # Provide SQL query section
        get_query()

        # If query result
        if st.session_state.query_result is not None:
            # Display query result
            query_result()
            # Display data download section
            data_download(file_ext)


if __name__ == "__main__":
    main()