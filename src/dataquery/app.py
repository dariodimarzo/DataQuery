import streamlit as st
import pandas as pd
import duckdb
from io import BytesIO
from re import sub, fullmatch, IGNORECASE, DOTALL
from os.path import splitext, basename
import zipfile
import csv
import hashlib
import pandavro as pdx
import json
from code_editor import code_editor


# Supported file extensions
FILE_EXT = ['avro', 'csv', 'json', 'parquet', 'txt', 'xlsx', 'xml', 'zip']
# Number of rows shown in the data preview
PREVIEW_ROWS = 5
# Hidden technical column holding the DuckDB rowid of editable query results
ROWID_COL = "_rowid_"
# csv quoting modes (names of the csv module constants)
QUOTING_OPTIONS = ['QUOTE_ALL', 'QUOTE_MINIMAL', 'QUOTE_NONNUMERIC', 'QUOTE_NONE']
# MIME types for result downloads
MIME_TYPES = {
    'csv': 'text/csv',
    'txt': 'text/plain',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'json': 'application/json',
    'xml': 'application/xml',
}
# A column reference in a SELECT list: column, "Column", table.column, t."Column"
COLUMN_REF = r'\s*(?:(?:"([^"]+)"|([A-Za-z_]\w*))\s*\.\s*)?(?:"([^"]+)"|([A-Za-z_]\w*))\s*'


# ---------------------------------------------------------------------------
# Session state
# ---------------------------------------------------------------------------

def default_session_state():
    """
    Returns the initial value of every session state variable (except the
    DuckDB connection, which is created separately).
    A function is used (instead of a constant) so that each call returns new
    mutable objects (dicts, sets, lists).
    Returns:
        dict: Session state variable names and their initial values.
    """
    return {
        # Catalog: table name -> source (file name, "(saved table)" or "(sql)")
        'tables': {},
        'saved_tables': set(),
        'sql_objects': set(),
        'table_signatures': {},
        # File upload
        'uploaded_files': [],
        'uploader_key': 0,
        'sheet_names_cache': {},
        # Query
        'query_statement': '',
        'last_submit_id': None,
        'query_result': None,
        # Edit
        'edited_df': None,
        'edit_source': None,
        'edit_rowids': [],
        'pending_edits': False,
    }


def init_session_state():
    """
    Initializes the session state variables not yet defined.
    Returns:
        None
    """
    for key, value in default_session_state().items():
        if key not in st.session_state:
            st.session_state[key] = value
    if 'con' not in st.session_state:
        st.session_state.con = duckdb.connect(database=':memory:')


def new_session():
    """
    Resets the whole session to a clean state.
    Recreates the in-memory DuckDB connection (which drops every table and view
    at once), resets all session variables and empties the file uploader by
    bumping its dynamic key. Triggers a rerun so the UI refreshes.
    Returns:
        None
    """
    uploader_key = st.session_state.uploader_key + 1
    for key, value in default_session_state().items():
        st.session_state[key] = value
    st.session_state.uploader_key = uploader_key
    st.session_state.con = duckdb.connect(database=':memory:')
    st.rerun()


# ---------------------------------------------------------------------------
# File upload and loading
# ---------------------------------------------------------------------------

def get_extension(file_name):
    """
    Returns the lowercase extension of a file name, without the dot.
    Args:
        file_name (str): The file name.
    Returns:
        str: The extension (e.g. 'csv'), or an empty string if missing.
    """
    return splitext(file_name)[1].lstrip('.').lower()


def get_file_size(file):
    """
    Returns the size in bytes of an uploaded or in-memory file.
    Args:
        file: An UploadedFile (exposes `.size`) or a BytesIO object.
    Returns:
        int: The file size in bytes.
    """
    size = getattr(file, 'size', None)
    return size if size is not None else len(file.getbuffer())


def upload_files():
    """
    Displays the file uploader and stores the uploaded files in the session.
    Tables of removed files are dropped later by files_to_db (cleanup step).
    Returns:
        list: The uploaded files.
    """
    uploaded_files = st.file_uploader(
        "Choose data files",
        accept_multiple_files=True,
        help='Upload your data files and zip archives.  \nAll files from zip archives and all sheets of xlsx files will be considered.',
        type=FILE_EXT,
        key=f"uploader_{st.session_state.uploader_key}",
    )
    for file in st.session_state.uploaded_files:
        if file not in uploaded_files:
            st.warning(f"Removed file: {file.name} and its associated tables")
    st.session_state.uploaded_files = uploaded_files
    return uploaded_files


def get_sheet_names(file, archive_name=None):
    """
    Returns the sheet names of an xlsx file, caching them in the session.
    Opening a workbook with pandas is expensive, so sheet names are read only
    the first time a given file (identified by archive, name and size) is seen.
    Args:
        file: The xlsx file object (UploadedFile or BytesIO).
        archive_name (str, optional): The zip archive name, if the file comes from a zip.
    Returns:
        list: The list of sheet names.
    """
    cache = st.session_state.sheet_names_cache
    key = (archive_name, file.name, get_file_size(file))
    if key not in cache:
        cache[key] = pd.ExcelFile(file).sheet_names
        file.seek(0)
    return cache[key]


def header_select(label, key):
    """
    Renders the Header (Yes/No) selectbox used in file settings.
    Args:
        label (str): The widget label.
        key (str): The widget key.
    Returns:
        int or None: 0 if the file has a header row, None otherwise (pandas convention).
    """
    return st.selectbox(
        label,
        [0, None],
        format_func=lambda x: "Yes" if x == 0 else "No",
        key=key,
        help="Yes: the first row (after skipped rows) contains column names.  \n"
             "No: columns are named col_1, col_2, ...",
    )


def rows_input(label, key, help_text):
    """
    Renders a non-negative integer input used for the skip rows settings.
    Args:
        label (str): The widget label.
        key (str): The widget key.
        help_text (str): The widget help text.
    Returns:
        int: The number entered by the user.
    """
    return int(st.number_input(label, min_value=0, value=0, step=1, key=key, help=help_text))


def alias_input(label, key):
    """
    Renders the Table alias text input used in file settings.
    Args:
        label (str): The widget label.
        key (str): The widget key.
    Returns:
        str: The alias entered by the user (empty for the default name).
    """
    return st.text_input(label, value="", key=key, help="Leave empty to use default name")


def get_file_options(file_name, sheet_names=None, archive_name=None):
    """
    Displays the settings of a file and returns the selected options.
    Args:
        file_name (str): The name of the file.
        sheet_names (list, optional): The sheet names, for xlsx files.
        archive_name (str, optional): The name of the zip archive, if any.
    Returns:
        dict: The selected options. For xlsx files, per-sheet options are in
            options['sheets'][sheet_name].
    Notes:
        - CSV/TXT: header, delimiter, quoting, quote character, skip rows, alias.
        - XLSX: sheets to load and, per sheet, header, skip rows and alias.
        - Other formats: alias only.
    """
    label = file_name if not archive_name else f"{archive_name} - {file_name}"
    key = f"{archive_name}_{file_name}"
    file_extension = get_extension(file_name)
    options = {}
    with st.expander(f"File Settings - {label}"):
        options['load'] = st.checkbox(
            "Load this file",
            value=True,
            key=f"load_{key}",
            help="Uncheck to exclude this file from the session (its tables are removed)",
        )
        if file_extension == 'xlsx':
            options['selected_sheets'] = st.multiselect(
                "Sheets to load",
                sheet_names,
                default=sheet_names,
                key=f"sheets_{key}",
                help="Each selected sheet is loaded as a separate table",
            )
            options['sheets'] = {}
            # One settings row per sheet
            for sheet in options['selected_sheets']:
                header_col, skiph_col, skipf_col, alias_col = st.columns([1, 1, 1, 2])
                with header_col:
                    header = header_select(f"Header - {sheet}", f"header_{key}_{sheet}")
                with skiph_col:
                    skiprows = rows_input(f"Skip header rows - {sheet}", f"skiprows_{key}_{sheet}",
                                          "Number of rows to skip at the top of the sheet, before the header")
                with skipf_col:
                    skipfooter = rows_input(f"Skip footer rows - {sheet}", f"skipfooter_{key}_{sheet}",
                                            "Number of rows to skip at the bottom of the sheet")
                with alias_col:
                    alias = alias_input(f"Table alias - {sheet}", f"alias_{key}_{sheet}")
                options['sheets'][sheet] = {
                    'header': header, 'skiprows': skiprows, 'skipfooter': skipfooter, 'alias': alias,
                }
            return options
        if file_extension in ['csv', 'txt']:
            left_column, right_column = st.columns(2)
            with left_column:
                options['header'] = header_select("Header", f"header_{key}")
                options['delimiter'] = st.text_input(
                    "Delimiter",
                    ",",
                    key=f"delimiter_{key}",
                    help="Character separating the fields, e.g. , or ; or |.  \nUse \\t for tab",
                )
                options['skiprows'] = rows_input("Skip header rows", f"skiprows_{key}",
                                                 "Number of rows to skip at the top of the file, before the header")
            with right_column:
                options['quoting'] = st.selectbox(
                    "Quoting",
                    QUOTING_OPTIONS,
                    key=f"quoting_{key}",
                    help="How quoted fields are handled:  \n"
                         "- QUOTE_ALL / QUOTE_MINIMAL / QUOTE_NONNUMERIC: quote characters are recognised and removed  \n"
                         "- QUOTE_NONE: quote characters are kept as part of the value",
                )
                options['quotechar'] = st.text_input(
                    "Quote character",
                    '"',
                    key=f"quote_{key}",
                    help="Character used to enclose fields containing the delimiter or line breaks",
                )
                options['skipfooter'] = rows_input("Skip footer rows", f"skipfooter_{key}",
                                                   "Number of rows to skip at the bottom of the file")
        alias_col, _ = st.columns(2)
        with alias_col:
            options['alias'] = alias_input("Table alias", f"alias_{key}")
    return options


def files_to_db():
    """
    Reconciles DuckDB with the uploaded files.
    Loads the uploaded files (including the supported files inside zip
    archives) as tables, then drops the tables no longer produced by any file
    (removed files, unselected sheets, renamed aliases). Tables saved from
    query results or created via SQL are preserved.
    Returns:
        None
    Notes:
        - Files with the same name uploaded more than once are ignored.
        - Files whose tables are already up to date are not read again.
    """
    loaded_msg = ""
    excluded_msg = ""
    loaded_tables = []
    # Tables not produced by files (kept by the cleanup)
    preserved = st.session_state.saved_tables | st.session_state.sql_objects
    # Names already in use, so default names can be disambiguated
    existing_names = set(preserved)
    uploaded_names = [f.name for f in st.session_state.uploaded_files]
    for file in st.session_state.uploaded_files:
        if uploaded_names.count(file.name) > 1:
            continue
        # Build the list of (file, archive name) to load
        if get_extension(file.name) == 'zip':
            to_load = []
            with zipfile.ZipFile(file) as z:
                for zip_info in z.infolist():
                    if zip_info.is_dir():
                        continue
                    if get_extension(zip_info.filename) not in FILE_EXT:
                        excluded_msg += f"{file.name} - {zip_info.filename} not loaded. Unsupported file format  \n"
                        continue
                    extracted_file = BytesIO(z.read(zip_info))
                    extracted_file.name = basename(zip_info.filename)
                    to_load.append((extracted_file, file.name))
        else:
            to_load = [(file, None)]
        # Load each file with its settings
        for data_file, archive_name in to_load:
            sheet_names = get_sheet_names(data_file, archive_name) if get_extension(data_file.name) == 'xlsx' else None
            options = get_file_options(data_file.name, sheet_names, archive_name)
            if not options['load']:
                continue
            tables = files_to_table(data_file, options, archive_name, existing_names)
            for table in tables:
                st.session_state.tables[table] = data_file.name
            loaded_tables += tables
            if tables:
                label = data_file.name if not archive_name else f"{archive_name} - {data_file.name}"
                tables_list = ''.join(f'  \n- {t}' for t in tables)
                loaded_msg += f"Loaded {label} as table(s):{tables_list}  \n\n"
    # Cleanup tables no longer produced by any file
    for table in list(st.session_state.tables):
        if table not in loaded_tables and table not in preserved:
            remove_table(st.session_state.con, table)
            forget_table(table)
    if loaded_msg:
        st.success(loaded_msg)
    if excluded_msg:
        st.warning(excluded_msg)


def files_to_table(file, options, archive_name, existing_names):
    """
    Loads a file (or the selected sheets of an xlsx file) as DuckDB tables.
    Each table has a signature describing the file and its load options: if a
    table with the same signature already exists it is reused, otherwise the
    data is read and the table is (re)created. Only the data actually needed
    is read (e.g. only the changed sheets of an xlsx file).
    Args:
        file: The file object (UploadedFile or BytesIO).
        options (dict): The load options (see get_file_options).
        archive_name (str or None): The zip archive name, if any.
        existing_names (set): Table names already in use. Updated in place
            with the names of the tables of this file.
    Returns:
        list: The names of the tables of this file (empty on error).
    """
    con = st.session_state.con
    # One table per selected sheet for xlsx files, one table otherwise (key None)
    keys = options['selected_sheets'] if get_extension(file.name) == 'xlsx' else [None]
    table_names = []
    to_read = []
    for key in keys:
        signature = table_signature(file, options, archive_name, key)
        cached = find_cached_table(signature, existing_names)
        if cached:
            existing_names.add(cached)
            table_names.append(cached)
        else:
            to_read.append((key, signature))
    if not to_read:
        return table_names
    try:
        dfs = read_file(file, options, [key for key, _ in to_read])
        for key, signature in to_read:
            key_options = options['sheets'][key] if key else options
            default_name = key if key else splitext(file.name)[0]
            label = f'sheet "{key}"' if key else file.name
            table_name = resolve_table_name(key_options.get('alias', ''), default_name, existing_names, label)
            if not table_name:
                continue
            existing_names.add(table_name)
            register_dataframe(con, dfs[key], table_name)
            st.session_state.table_signatures[table_name] = signature
            table_names.append(table_name)
    except Exception as e:
        if "Error tokenizing data" in str(e):
            st.warning(f'{file.name} not loaded. Please check file settings.')
        else:
            st.error(f"Error loading file {file.name}: {e}")
        return []
    return table_names


def read_file(file, options, sheets):
    """
    Reads a file into DataFrames using the selected options.
    Args:
        file: The file object (UploadedFile or BytesIO).
        options (dict): The load options (see get_file_options).
        sheets (list): The xlsx sheets to read. Ignored for other formats.
    Returns:
        dict: {sheet_name: DataFrame} for xlsx files, {None: DataFrame} otherwise.
    """
    file_extension = get_extension(file.name)
    if file_extension in ['csv', 'txt']:
        skipfooter = options['skipfooter']
        # skipfooter is supported only by the python engine, so the (faster)
        # C engine is used when it is not needed
        df = pd.read_csv(
            file,
            sep='\t' if options['delimiter'] == '\\t' else options['delimiter'],
            quoting=getattr(csv, options['quoting']),
            quotechar=options['quotechar'],
            header=options['header'],
            skiprows=options['skiprows'],
            skipfooter=skipfooter,
            engine='python' if skipfooter > 0 else 'c',
            dtype=str,
            on_bad_lines='skip',
            encoding_errors='ignore',
        )
        return {None: name_columns(df, options['header'])}
    if file_extension == 'xlsx':
        xls = pd.ExcelFile(file)
        dfs = {}
        for sheet in sheets:
            sheet_options = options['sheets'][sheet]
            df = pd.read_excel(
                xls,
                sheet_name=sheet,
                header=sheet_options['header'],
                skiprows=sheet_options['skiprows'],
                skipfooter=sheet_options['skipfooter'],
            )
            dfs[sheet] = name_columns(df, sheet_options['header'])
        return dfs
    if file_extension == 'parquet':
        return {None: pd.read_parquet(file)}
    if file_extension == 'avro':
        return {None: pdx.read_avro(file, na_dtypes=True)}
    if file_extension == 'json':
        json_data = json.load(file)
        # A single object or a list of objects
        df = pd.DataFrame([json_data]) if isinstance(json_data, dict) else pd.json_normalize(json_data)
        return {None: df}
    if file_extension == 'xml':
        return {None: pd.read_xml(file)}
    raise ValueError("Unsupported file format")


def name_columns(df, header):
    """
    Names the columns col_1, col_2, ... when the file has no header row.
    Args:
        df (pd.DataFrame): The DataFrame read from the file.
        header (int or None): The header option (None means no header row).
    Returns:
        pd.DataFrame: The DataFrame, with renamed columns if there is no header.
    """
    if header is None:
        df.columns = [f'col_{i + 1}' for i in range(len(df.columns))]
    return df


def table_signature(file, options, archive_name=None, sheet_name=None):
    """
    Builds a signature describing a source file and its load options.
    The signature is used to decide whether a table must be rebuilt on rerun:
    it combines the file identity (name, archive, size) with the load options
    of the table, so that changing any of them triggers a rebuild.
    Args:
        file: The file object.
        options (dict): The load options of the file.
        archive_name (str, optional): The zip archive name, if any.
        sheet_name (str, optional): The sheet name, for xlsx files.
    Returns:
        str: A stable JSON signature string.
    """
    if sheet_name is not None:
        table_options = options['sheets'][sheet_name]
    else:
        table_options = {k: v for k, v in options.items() if k != 'load'}
    payload = {
        'file': file.name,
        'archive': archive_name,
        'size': get_file_size(file),
        'sheet': sheet_name,
        'options': table_options,
    }
    return json.dumps(payload, sort_keys=True, default=str)


def find_cached_table(signature, existing_names):
    """
    Looks for an already loaded table matching a load signature.
    Args:
        signature (str): Signature describing the source file and its load options.
        existing_names (set): Names already claimed in the current run.
    Returns:
        str or None: The name of the matching table, or None if not found.
    """
    for table_name, table_sig in st.session_state.table_signatures.items():
        if table_sig == signature and table_name not in existing_names:
            return table_name
    return None


def resolve_table_name(alias, default_name, existing_names, label):
    """
    Resolves the name of a new table.
    An explicit alias is used as-is (cleaned) and must not be already in use.
    Without alias, the default name is used, with a numeric suffix if needed.
    Args:
        alias (str): The alias entered by the user (may be empty).
        default_name (str): The default name (file name without extension or sheet name).
        existing_names (set): Names already in use.
        label (str): Description of the source, used in error messages.
    Returns:
        str or None: The table name, or None if the alias is invalid or already in use.
    """
    alias = alias.strip()
    if not alias:
        return resolve_unique_name(default_name, existing_names)
    table_name = clean_table_name(alias)
    if not table_name:
        st.error(f"Invalid alias for {label}. Skipping.")
        return None
    if table_name in existing_names:
        st.error(f"Duplicate alias: {label} resolves to table \"{table_name}\", already in use. Skipping.")
        return None
    return table_name


def clean_table_name(name):
    """
    Cleans a table name: lowercase, without spaces and special characters.
    Args:
        name (str): The table name to be cleaned.
    Returns:
        str: The cleaned table name.
    """
    return sub(r'[^a-z0-9_]', '', name.lower())


def resolve_unique_name(name, existing_names):
    """
    Returns a cleaned table name not in use, adding a numeric suffix if needed.
    Args:
        name (str): The candidate name (raw, not yet cleaned).
        existing_names (set): Names already in use.
    Returns:
        str: A cleaned, unique table name.
    """
    base = clean_table_name(name) or 'table'
    candidate, i = base, 2
    while candidate in existing_names:
        candidate, i = f"{base}_{i}", i + 1
    return candidate


# ---------------------------------------------------------------------------
# DuckDB catalog
# ---------------------------------------------------------------------------

def register_dataframe(con, df, table_name):
    """
    Materializes a DataFrame as a writable base table in DuckDB.
    The DataFrame is exposed through a temporary view and then copied into a
    real table with CREATE OR REPLACE TABLE ... AS SELECT.
    Args:
        con (Connection): The DuckDB connection object.
        df (pd.DataFrame): The DataFrame to materialize.
        table_name (str): The name of the table.
    Returns:
        None
    """
    tmp_view = f"_src_{table_name}"
    con.register(tmp_view, df)
    try:
        con.execute(f'CREATE OR REPLACE TABLE "{table_name}" AS SELECT * FROM "{tmp_view}"')
    finally:
        con.unregister(tmp_view)


def remove_table(con, table_name):
    """
    Drops a table from DuckDB, showing a warning if it cannot be dropped.
    Args:
        con (Connection): The DuckDB connection object.
        table_name (str): The name of the table to remove.
    Returns:
        None
    """
    try:
        con.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    except duckdb.CatalogException as e:
        st.warning(f"Could not drop table {table_name}: {e}")


def forget_table(table_name):
    """
    Removes a table from every app catalog (not from DuckDB).
    Args:
        table_name (str): The name of the table.
    Returns:
        None
    """
    st.session_state.tables.pop(table_name, None)
    st.session_state.saved_tables.discard(table_name)
    st.session_state.sql_objects.discard(table_name)
    st.session_state.table_signatures.pop(table_name, None)


def sync_catalog(con):
    """
    Reconciles the app catalog with the tables and views actually in DuckDB.
    Objects created via SQL (CREATE TABLE/VIEW) are added to the catalog and
    tracked in st.session_state.sql_objects (so the file cleanup keeps them);
    objects dropped via SQL are removed from the catalog. Internal temporary
    views (prefixed with '_src_') are ignored.
    Args:
        con (Connection): The DuckDB connection object.
    Returns:
        bool: True if the catalog changed, False otherwise.
    """
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    live = {name for (name,) in rows if not name.startswith('_src_')}
    changed = False
    for name in live - set(st.session_state.tables):
        st.session_state.tables[name] = "(sql)"
        st.session_state.sql_objects.add(name)
        changed = True
    for name in set(st.session_state.tables) - live:
        forget_table(name)
        changed = True
    return changed


# ---------------------------------------------------------------------------
# Preview and session controls
# ---------------------------------------------------------------------------

def data_preview():
    """
    Displays the first rows of the selected table.
    Returns:
        None
    """
    with st.expander("Data Preview", expanded=False):
        select_col, _ = st.columns(2)
        with select_col:
            table_name = st.selectbox('Select Table:', list(st.session_state.tables))
        df = st.session_state.con.execute(f'SELECT * FROM "{table_name}" LIMIT {PREVIEW_ROWS}').fetchdf()
        df.index = range(1, len(df) + 1)
        st.dataframe(df)


def build_session_zip(con, table_names):
    """
    Builds an in-memory ZIP archive containing one parquet file per table.
    Each table is exported as "<table_name>.parquet", so re-uploading the ZIP
    restores the session with the same table names. Nothing is written to the
    server filesystem.
    Notes:
        - Runs inside a deferred st.download_button callable, on a separate
          thread: it must not use st.* commands or st.session_state, so
          connection and table list are passed in.
        - A dedicated cursor is used because DuckDB connections should not be
          shared across threads.
        - Tables that cannot be exported are skipped.
    Args:
        con (Connection): The DuckDB connection object.
        table_names (list): Names of the tables to export.
    Returns:
        bytes: The ZIP archive.
    """
    cur = con.cursor()
    zip_buffer = BytesIO()
    try:
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for table_name in table_names:
                try:
                    df = cur.execute(f'SELECT * FROM "{table_name}"').fetchdf()
                except Exception:
                    continue
                parquet_buffer = BytesIO()
                df.to_parquet(parquet_buffer, index=False, engine='pyarrow')
                zf.writestr(f"{table_name}.parquet", parquet_buffer.getvalue())
    finally:
        cur.close()
    return zip_buffer.getvalue()


def session_controls():
    """
    Renders the session buttons on one row:
    - New Session drops every table/view and clears the session.
    - Save Session downloads all tables as a ZIP of parquet files. The ZIP is
      generated only on click (callable passed as `data`), so it adds no cost
      to normal reruns; on_click="ignore" avoids rerunning the app.
    Returns:
        None
    """
    col_new, col_save, _ = st.columns([1, 1, 5])
    with col_new:
        if st.button("New Session", help="Remove all objects and start from a clean session", use_container_width=True):
            new_session()
    with col_save:
        con = st.session_state.con
        table_names = list(st.session_state.tables)
        st.download_button(
            label="Save Session",
            data=lambda: build_session_zip(con, table_names),
            file_name="dataquery_session.zip",
            mime="application/zip",
            help="Download all session tables as a ZIP of parquet files.  \n"
                 "Re-upload the ZIP to restore the session.",
            on_click="ignore",
            use_container_width=True,
        )


# ---------------------------------------------------------------------------
# Query
# ---------------------------------------------------------------------------

def sql_completions():
    """
    Builds the autocompletion list (tables and columns) for the code editor.
    Table and column names are read with a single query. Column suggestions
    are de-duplicated when the same column appears in more tables.
    Returns:
        list: The completions in the format expected by code_editor.
    """
    tables = set(st.session_state.tables)
    rows = st.session_state.con.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_schema = 'main' ORDER BY table_name, ordinal_position"
    ).fetchall()
    completions = [
        {"caption": t, "value": f'"{t}"', "meta": "table", "name": f"table_{t}", "score": 200}
        for t in sorted(tables)
    ]
    seen_columns = set()
    for table_name, column_name in rows:
        if table_name in tables and column_name not in seen_columns:
            seen_columns.add(column_name)
            completions.append({"caption": column_name, "value": f'"{column_name}"', "meta": "column",
                                "name": f"col_{column_name}", "score": 100})
    return completions


def get_query():
    """
    Displays the SQL editor (with table/column autocompletion) and runs the
    query on submit (Ctrl + Enter or Run button).
    Returns:
        None
    """
    st.subheader("Query Data")
    st.caption("Type your SQL query below and press \"Ctrl + Enter\" to run it.")
    completions = sql_completions()
    query_btn = [{
        "name": "Run",
        "feather": "Play",
        "primary": True,
        "hasText": True,
        "showWithIcon": True,
        "commands": ["saveState", "submit"],
        "style": {"bottom": "0.44rem", "right": "0.4rem"},
        "alwaysOn": True
    }]
    # The key changes (and the editor is remounted) only when completions change
    comp_hash = hashlib.md5(str(completions).encode()).hexdigest()
    sql_query_input = code_editor(
        code=st.session_state.query_statement,
        lang='sql',
        shortcuts="vscode",
        options={"enableBasicAutocompletion": True,
                 "enableLiveAutocompletion": True,
                 "showLineNumbers": True,
                 "highlightActiveLine": True,
                 "highlightSelectedWord": True},
        height=[5, 7],
        buttons=query_btn,
        focus=True,
        completions=completions,
        key=f'sql_query_{comp_hash}',
        allow_reset=False
    )
    # The editor returns its last response on every rerun: each submit is
    # identified by its event id, so the query runs once per submit (and the
    # same text can be run again)
    submit_id = sql_query_input.get("id")
    if (sql_query_input.get("type") == "submit" and submit_id is not None
            and submit_id != st.session_state.last_submit_id):
        st.session_state.last_submit_id = submit_id
        st.session_state.query_statement = sql_query_input["text"]
        # Refresh the UI (preview, autocompletion, ...) when the catalog changed
        if execute_statement(st.session_state.query_statement):
            st.rerun()


def execute_statement(sql_query):
    """
    Executes a SQL statement and stores its result in the session.
    Editable queries (see get_editable_query) are rewritten to also return the
    DuckDB rowid in a hidden column, so edits can later be applied row by row
    to the source table.
    Args:
        sql_query (str): The SQL statement to execute.
    Returns:
        bool: True if the catalog changed (CREATE/DROP via SQL), False otherwise.
    """
    st.session_state.query_result = None
    st.session_state.edited_df = None
    st.session_state.edit_source = None
    st.session_state.edit_rowids = []
    st.session_state.pending_edits = False
    if not sql_query.strip():
        return False
    con = st.session_state.con
    try:
        editable = get_editable_query(con, sql_query)
        source, query = editable if editable else (None, sql_query)
        result_df = con.execute(query).fetchdf()
    except Exception as e:
        err = str(e)
        if "already exists" in err:
            st.error("Object already existing. Please choose a different name or use CREATE OR REPLACE.")
        elif "Catalog Error: Table with name" in err and "does not exist" in err:
            st.error("Table not existing. Please check table names in your query.")
        elif "Can only update base table" in err:
            st.error("Update not available. Please consider a different select statement and the edit mode.")
        else:
            st.error(f"Error executing query: {err}")
        return False
    # Index starting from 1 for display
    result_df.index = range(1, len(result_df) + 1)
    st.session_state.query_result = result_df
    if source:
        st.session_state.edit_source = source
        st.session_state.edit_rowids = result_df[ROWID_COL].astype('int64').tolist()
    # Reflect any CREATE/DROP TABLE/VIEW issued via SQL in the UI catalog
    return sync_catalog(con)


def strip_sql_comments(sql_query):
    """
    Removes SQL comments (-- ... and /* ... */) from a query, ignoring
    comment markers inside single-quoted strings.
    Args:
        sql_query (str): The SQL query.
    Returns:
        str: The query without comments.
    """
    return sub(
        r"('(?:[^']|'')*')|--[^\n]*|/\*.*?\*/",
        lambda m: m.group(1) or ' ',
        sql_query,
        flags=DOTALL,
    )


def parse_column_list(column_list):
    """
    Parses the column list of a SELECT into plain column references.
    Accepted items are column names, optionally double-quoted and optionally
    qualified by a table name or alias (e.g. col, "My Col", t.col).
    Expressions, functions, aliases (AS), DISTINCT and any other construct are
    rejected, because such columns cannot be written back.
    Args:
        column_list (str): The text between SELECT and FROM.
    Returns:
        list or None: (qualifier, column_name) tuples (qualifier None if
            absent), or None if the list contains non-plain items.
    """
    columns = []
    for item in column_list.split(','):
        m = fullmatch(COLUMN_REF, item)
        if not m:
            return None
        columns.append((m.group(1) or m.group(2), m.group(3) or m.group(4)))
    return columns


def get_editable_query(con, sql_query):
    """
    Checks whether a query can be edited and applied back to its source table.
    Editable queries select from a single base table, optionally followed by
    WHERE, ORDER BY and/or LIMIT, and return either all columns (SELECT *) or
    a list of plain columns (see parse_column_list), each once. Comments, the
    "main." schema prefix, quoted names and a table alias are accepted, and
    names are matched case-insensitively (as DuckDB does).
    The query is rewritten to also return the DuckDB rowid as a technical
    column, which identifies each row in the table, and to use the actual
    table column names. Views, joins, expressions, column aliases, DISTINCT
    and aggregations are not editable.
    Args:
        con (Connection): The DuckDB connection object.
        sql_query (str): The SQL query entered by the user.
    Returns:
        tuple or None: (table_name, rewritten_query) if editable, None otherwise.
    """
    match = fullmatch(
        r'\s*select\s+(.+?)\s+from\s+'
        r'(?:"?main"?\s*\.\s*)?'                                                  # optional schema
        r'(?:"([^"]+)"|([A-Za-z_]\w*))'                                           # table name
        r'(\s+(?:as\s+)?(?!where\b|order\b|limit\b)([A-Za-z_]\w*))?'              # optional alias
        r'((?:\s+(?:where|order\s+by|limit)\b.*?)?)'                              # optional clauses
        r'\s*;?\s*',
        strip_sql_comments(sql_query),
        flags=IGNORECASE | DOTALL,
    )
    if not match:
        return None
    column_list, typed_name, alias_clause, alias_name, clauses = (
        match.group(1), match.group(2) or match.group(3), match.group(4) or '', match.group(5), match.group(6)
    )
    # Base table only (views have no rowid), matched case-insensitively.
    # fetchall (not fetchone) fully consumes the result and frees the connection
    rows = con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' AND lower(table_name) = lower(?) AND table_type = 'BASE TABLE'",
        [typed_name],
    ).fetchall()
    if not rows:
        return None
    table_name = rows[0][0]
    from_clause = f'FROM "{table_name}"{alias_clause}{clauses}'
    if column_list.strip() == '*':
        return table_name, f'SELECT rowid AS "{ROWID_COL}", * {from_clause}'
    parsed = parse_column_list(column_list)
    if not parsed:
        return None
    table_columns = {r[0].lower(): r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ?",
        [table_name],
    ).fetchall()}
    valid_qualifiers = {typed_name.lower(), table_name.lower(), (alias_name or '').lower()}
    selected = []
    for qualifier, column in parsed:
        actual = table_columns.get(column.lower())
        if (qualifier and qualifier.lower() not in valid_qualifiers) or actual is None or actual in selected:
            return None
        selected.append(actual)
    columns_sql = ', '.join(f'"{c}"' for c in selected)
    return table_name, f'SELECT rowid AS "{ROWID_COL}", {columns_sql} {from_clause}'


# ---------------------------------------------------------------------------
# Query result: display, edit, apply, save, download
# ---------------------------------------------------------------------------

def current_result():
    """
    Returns the result currently shown to the user, including any edits.
    Returns:
        pd.DataFrame: The edited result while in Edit Mode, otherwise the query result.
    """
    if st.session_state.edited_df is not None:
        return st.session_state.edited_df
    return st.session_state.query_result


def export_result():
    """
    Returns the current result without the technical rowid column, for
    downloads and Save as Table.
    Returns:
        pd.DataFrame: The current result to export.
    """
    return current_result().drop(columns=[ROWID_COL], errors='ignore')


def mark_pending_edits():
    """
    Data editor callback: flags that the result contains unapplied edits.
    Streamlit calls it only when the user changes a cell, adds or deletes a
    row, so detecting edits has no cost on normal reruns.
    Returns:
        None
    """
    st.session_state.pending_edits = True


def query_result():
    """
    Displays the query result, with an Edit Mode to change the data.
    When Edit Mode is turned off, the edits are kept in the displayed result
    (and stay available for Apply to Table until a new query is run).
    Returns:
        None
    """
    st.subheader("Query Result")
    if st.session_state.query_result.empty:
        st.info("There is no data to display.")
    if st.toggle("Edit Mode", key="edit_mode"):
        st.session_state.edited_df = st.data_editor(
            st.session_state.query_result,
            num_rows="dynamic",
            column_config={ROWID_COL: None},  # hidden technical column
            on_change=mark_pending_edits,
        )
    else:
        # Keep the edits made in Edit Mode in the displayed result
        if st.session_state.edited_df is not None:
            st.session_state.query_result = st.session_state.edited_df
            st.session_state.edited_df = None
        st.dataframe(st.session_state.query_result, column_config={ROWID_COL: None})


def apply_edits_to_table():
    """
    Renders the Apply to Table button, which applies the edits to the source table.
    Changes are applied using the hidden rowid column, so rows not returned
    by the query are left untouched:
        - modified rows are updated (only the displayed columns),
        - rows deleted in the editor are deleted from the table,
        - rows added in the editor are inserted (columns not displayed are NULL).
    All operations run in a single transaction. After applying, the query is
    executed again so the result reflects the table content.
    The button is enabled only for editable queries (see get_editable_query).
    Returns:
        None
    """
    source = st.session_state.edit_source
    if source:
        help_text = (f"Apply the changes to table '{source}'.  \n"
                     "Only the displayed columns are updated; deleted rows are removed entirely.")
    else:
        help_text = ("Available only for SELECT * or SELECT col1, col2, ... FROM <table> "
                     "[WHERE ...] [ORDER BY ...] [LIMIT ...], without expressions, aliases or joins")
    if not st.button("Apply to Table", type="primary", disabled=not source, help=help_text,
                     use_container_width=True):
        return
    con = st.session_state.con
    edited = current_result()
    columns = [c for c in edited.columns if c != ROWID_COL]
    columns_sql = ', '.join(f'"{c}"' for c in columns)
    set_clause = ', '.join(f'"{c}" = e."{c}"' for c in columns)
    # Rows of the original result no longer in the edited result were deleted
    kept_ids = set(edited[ROWID_COL].dropna().astype('int64'))
    deleted_ids = [str(i) for i in st.session_state.edit_rowids if i not in kept_ids]
    tmp_view = "_src_edits"
    con.register(tmp_view, edited)
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute(
            f'UPDATE "{source}" SET {set_clause} FROM "{tmp_view}" e '
            f'WHERE "{source}".rowid = CAST(e."{ROWID_COL}" AS BIGINT)'
        )
        if deleted_ids:
            con.execute(f'DELETE FROM "{source}" WHERE rowid IN ({", ".join(deleted_ids)})')
        # New rows have no rowid yet
        con.execute(
            f'INSERT INTO "{source}" ({columns_sql}) '
            f'SELECT {columns_sql} FROM "{tmp_view}" WHERE "{ROWID_COL}" IS NULL'
        )
        con.execute("COMMIT")
    except Exception as e:
        con.execute("ROLLBACK")
        st.error(f"Could not apply changes to '{source}': {e}")
        return
    finally:
        con.unregister(tmp_view)
    execute_statement(st.session_state.query_statement)
    st.toast(f"Changes applied to table '{source}'.")
    st.rerun()


def save_as_table():
    """
    Saves the current result (with edits) as a new session table.
    The table is tracked in st.session_state.saved_tables, so it is kept by
    the file cleanup and included in the session export.
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
            table_name = clean_table_name(new_name)
            if not table_name:
                st.warning("Please provide a valid table name.")
                return
            if table_name in st.session_state.tables:
                st.error(f"Table '{table_name}' already exists. Choose a different name.")
                return
            register_dataframe(st.session_state.con, export_result(), table_name)
            st.session_state.tables[table_name] = "(saved table)"
            st.session_state.saved_tables.add(table_name)
            st.rerun()


def df_to_file(df, file_format, **kwargs):
    """
    Converts a DataFrame to the requested file format.
    Args:
        df (pd.DataFrame): The DataFrame to convert.
        file_format (str): The file format.
        **kwargs: Additional arguments for the pandas writer (csv/txt, xlsx).
    Returns:
        bytes: The file content.
    """
    buffer = BytesIO()
    if file_format in ['csv', 'txt']:
        df.to_csv(buffer, index=False, **kwargs)
    elif file_format == 'xlsx':
        df.to_excel(buffer, index=False, engine='openpyxl', **kwargs)
    elif file_format == 'json':
        df.to_json(buffer, orient='records')
    elif file_format == 'parquet':
        df.to_parquet(buffer, index=False, engine='pyarrow')
    elif file_format == 'xml':
        df.to_xml(buffer, index=False)
    elif file_format == 'avro':
        pdx.to_avro(buffer, df)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")
    return buffer.getvalue()


def data_download():
    """
    Displays the Data Download popover (format and options) with a single
    Download button. The file is generated only on click (callable passed as
    `data`), so it adds no cost to normal reruns.
    Returns:
        None
    """
    with st.popover("Data Download", use_container_width=True):
        file_format = st.selectbox("Select file format:", [ext for ext in FILE_EXT if ext != 'zip'])
        kwargs = {}
        if file_format in ['csv', 'txt']:
            header = st.selectbox("Header:", ("Y", "N"))
            delimiter = st.text_input("Delimiter:", max_chars=1, value=",")
            quoting = st.selectbox("Quoting:", QUOTING_OPTIONS)
            kwargs = {'sep': delimiter or ',', 'quoting': getattr(csv, quoting), 'header': header == 'Y'}
            # With QUOTE_NONE special characters are escaped instead of raising an error
            if quoting == 'QUOTE_NONE':
                kwargs['escapechar'] = '\\'
        elif file_format == 'xlsx':
            header = st.selectbox("Header:", ("Y", "N"))
            kwargs = {'header': header == 'Y'}
        df = export_result()
        st.download_button(
            label="Download",
            data=lambda: df_to_file(df, file_format, **kwargs),
            file_name=f"query_result.{file_format}",
            mime=MIME_TYPES.get(file_format, 'application/octet-stream'),
            on_click="ignore",
        )


def result_actions():
    """
    Renders the actions on the query result:
    - Apply to Table, on its own row, only while there are unapplied edits;
    - Save as Table and Data Download.
    Returns:
        None
    """
    if st.session_state.pending_edits:
        apply_col, _ = st.columns([1, 6])
        with apply_col:
            apply_edits_to_table()
    save_col, download_col, _ = st.columns([1, 1, 5])
    with save_col:
        save_as_table()
    with download_col:
        data_download()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """
    The main function of the DataQuery application: file upload and loading,
    data preview, SQL query, result editing and export.
    Returns:
        None
    """
    st.set_page_config(page_title='DataQuery', page_icon=":material/table:", layout="wide")
    st.title("DataQuery")
    st.subheader("Preview, query, edit and export data files")
    init_session_state()
    upload_files()
    # Load new files and drop tables of removed files
    files_to_db()
    # Show the working UI whenever there is at least one table
    if not st.session_state.tables:
        return
    saved_tabs = [t for t in st.session_state.tables if t in st.session_state.saved_tables]
    if saved_tabs:
        st.success("Saved query result as table(s):" + ''.join(f'  \n- {t}' for t in saved_tabs))
    sql_tabs = [t for t in st.session_state.tables if t in st.session_state.sql_objects]
    if sql_tabs:
        st.success("Created table(s) via SQL:" + ''.join(f'  \n- {t}' for t in sql_tabs))
    data_preview()
    session_controls()
    get_query()
    if st.session_state.query_result is not None:
        query_result()
        result_actions()


if __name__ == "__main__":
    main()
