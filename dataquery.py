import streamlit as st
import pandas as pd
import duckdb
from io import BytesIO
from re import sub
from os.path import splitext,basename
#manage zip files
import zipfile
#manage csv
import csv
#manage avro
import pandavro as pdx
import json
#import avro.schema
#from avro.datafile import DataFileReader
#from avro.io import DatumReader


def upload_files(file_ext):
    """
    Uploads data files and zip archives.

    Args:
        file_ext (str): The file extension to filter the uploaded files.

    Returns:
        st.session_state.uploaded_files (list): A list of uploaded files.

    Raises:
        None

    """
    #upload files object
    uploaded_files = st.file_uploader("Choose data files", accept_multiple_files=True,
        help='Upload your data files and zip archives.  \nAll files from zip archives and all sheets of xlsx files will be considered.',
        type=file_ext)

    # Check for removed files
    removed_files = [file for file in st.session_state.uploaded_files if file not in uploaded_files]
    #remove view from db for removed files
    for file in removed_files:
        tables_to_remove = [table for table, source in st.session_state.tables.items() if source == file.name]
        for table in tables_to_remove:
            remove_view(st.session_state.con, table)
            del st.session_state.tables[table]
        st.warning(f"Removed file: {file.name} and its associated tables")

    # Update the list of uploaded files
    st.session_state.uploaded_files = uploaded_files

    return st.session_state.uploaded_files

def remove_view(con,view_name):
    """
    Safely remove a view from the DuckDB connection.

    Args:
        con (Connection): The DuckDB connection object.
        view_name (str): The name of the view to remove.
    
    Returns:
        None
    """

    #remove view from db
    try:
        con.execute(f"DROP VIEW IF EXISTS {view_name}")
    except duckdb.CatalogException as e:
        st.warning(f"Could not drop view {view_name}: {str(e)}")

def files_to_db(file_ext):
    """
    Load files into a database and register them as tables.

    Args:
        file_ext (list): A list of file extensions to be loaded.

    Returns:
        st.session_state.tables (Dict): A dictionary mapping table names to file names.

    """
    loaded_tab = ""
    excluded_tab = ""
    file_options = {}

    for file in  st.session_state.uploaded_files:
        if file.name not in [f.name for f in st.session_state.uploaded_files if f != file]:
            file_extension = file.name.split('.')[-1].lower()
            
            #manage zip archives
            if file.type == "application/x-zip-compressed":
                with zipfile.ZipFile(file) as z:
                    for zip_info in z.infolist():
                        if not zip_info.is_dir():
                            _, extension = splitext(zip_info.filename)
                            if extension.lstrip('.').lower() in file_ext:
                                with z.open(zip_info) as zf:
                                    extracted_file = BytesIO(zf.read())
                                    extracted_file.name = basename(zip_info.filename)
                                    #get file options
                                    options = get_file_options(extracted_file.name, None if extension.lstrip('.').lower() != 'xlsx' else pd.ExcelFile(extracted_file).sheet_names, file.name)
                                    
                                    file_options[extracted_file.name] = options
                                    #load tables registering view in duckdb
                                    loaded_tables = files_to_table(extracted_file, st.session_state.con, options, file.name)
                                    if loaded_tables:
                                        for table in loaded_tables:
                                            st.session_state.tables[table] = extracted_file.name
                                        loaded_tab += f"Loaded {file.name} - {extracted_file.name} as table(s): {', '.join(loaded_tables)}  \n"
                            else:
                                excluded_tab += f"{file.name} - {zip_info.filename} not loaded. Unsupported file format  \n"
            #manage single files
            else:
                #get file options
                options = get_file_options(file.name, None if file_extension != 'xlsx' else pd.ExcelFile(file).sheet_names)
                
                #load tables registering view in duckdb
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


def get_file_options(file_name, sheet_names = None, archive_name = None):
    """
    Get file options for csv, txt, xlsx files and return the selected options.

    Args:
        file_name (str): Name of the file.
        sheet_names (list): List of sheet names for Excel files.
        archive_name: Name of the zip archive.

    Returns:
        options (Dict): Selected options for the file or sheets.
    """
    
    options = {}
    label_obj = f"{file_name}" if not archive_name else f"{archive_name} - {file_name}"
    #get file extension
    file_extension = file_name.split('.')[-1].lower()
    #get options only for csv,txt,xlsx
    if file_extension in ['csv', 'txt', 'xlsx']:
        with st.expander(f"File Settings - {label_obj}"):
            #if extension is xlsx get header option
            if file_extension == 'xlsx':
                #use two columns layout
                left_column, right_column = st.columns(2)
                options['sheets'] = {}
                for index, sheet in enumerate(sheet_names):
                #loop all xlsx sheets:
                    with left_column if index % 2 == 0 else right_column:
                        options['sheets'][sheet] = {
                            'header': st.selectbox(f"Header for {file_name} - {sheet}", [0, None], format_func=lambda x: "Yes" if x == 0 else "No",key=f"header_{archive_name}_{file_name}_{sheet}")
                        }
            #if extension csv, txt get header, delimiter, quoting and quoting char
            elif file_extension in ['csv', 'txt']:
                #use two columns layout
                left_column, right_column = st.columns(2)
                with left_column:
                    options['header'] = st.selectbox(f"Header", [0, None], format_func=lambda x: "Yes" if x == 0 else "No",key=f"header_{archive_name}_{file_name}")
                    options['delimiter'] = st.text_input(f"Delimiter", ",",key=f"delimiter_{archive_name}_{file_name}")
                with right_column:
                    quoting_options = {                    
                        'QUOTE_ALL': csv.QUOTE_ALL,
                        'QUOTE_MINIMAL': csv.QUOTE_MINIMAL,
                        'QUOTE_NONNUMERIC': csv.QUOTE_NONNUMERIC,
                        'QUOTE_NONE': csv.QUOTE_NONE
                    }
                    options['quoting'] = st.selectbox(f"Quoting", list(quoting_options.keys()),key=f"quoting_{archive_name}_{file_name}")
                    options['quotechar'] = st.text_input(f"Quote character", '"',key=f"quote_{archive_name}_{file_name}")
    
    return options

def files_to_table(file, con, options= None, archive_name = None):

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
    
    #get file extension file name and archive name
    file_extension = file.name.split('.')[-1].lower()
    file_nm = f"{archive_name.replace('.','_')}_{file.name.replace('.','_')}" if archive_name else file.name.replace('.','_')
    table_names = []

    try:
        #manage csv and txt using collected file settings
        if file_extension in ['csv', 'txt']:
            delim = '\t' if options['delimiter'] == '\\t' else options['delimiter']
            
            df = pd.read_csv(file,
                             sep=delim,
                             quoting=getattr(csv, options.get('quoting', 'QUOTE_NONE')),
                             quotechar=options.get('quotechar', '"'),
                             header=options.get('header', 0))
            #in case of no header rename columns from simple integer to col_integer
            if options.get('header', 0) is None:
                 df.columns = [f'col_{i+1}' for i in range(len(df.columns))] 
        #manage xlsx using header settings collected
        elif file_extension == 'xlsx':
            xls = pd.ExcelFile(file)
            dfs = {}
            #loop all sheets and in case of no header rename columns from simple integer to col_integer
            for sheet_name in xls.sheet_names:
                sheet_options = options.get('sheets', {}).get(sheet_name, {})
                dfs[sheet_name] = pd.read_excel(file, sheet_name=sheet_name, header=sheet_options.get('header', 0))
                if sheet_options.get('header', 0) is None:
                    dfs[sheet_name].columns = [f'col_{i+1}' for i in range(len(dfs[sheet_name].columns))]  
        #manage other accepted file formats
        elif file_extension == 'parquet':
            df = pd.read_parquet(file)
        elif file_extension == 'avro':
            df = pdx.read_avro(file, na_dtypes=True)
        elif file_extension == 'json':
            json_data=json.load(file)
            if isinstance(json_data, dict):
                # It's a single object
                df = pd.DataFrame([json_data])
            else:
                # It's a list of objects
                df = pd.json_normalize(json_data)
            
            #json_data = pd.read_json(file)
            #df = pd.json_normalize(json_data.to_dict('records')) if isinstance(json_data, pd.DataFrame) else pd.json_normalize(json_data.to_dict())
        elif file_extension == 'xml':
            df = pd.read_xml(file)
        else:
            st.error(f"File {file.name} not loaded. Unsupported file format.")
            return None

        #register dataframes into duckdb
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
        #manage exception of wrong file settings provided for csv and txt
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

    #clean table name
    table_name = clean_table_name(file_name)

    #register view in db
    con.register(table_name, df)
    return table_name

def clean_table_name(name):
    """
    Cleans the table name by replacing spaces with underscores and removing all other special characters.

    Args:
        name (str): The table name to be cleaned.

    Returns:
        name(str): The cleaned table name.
    """

    #replace spaces with underscores
    name = name.replace(' ', '').lower()
    #remove all other special characters
    name = sub(r'[^a-zA-Z0-9_]', '', name)
    return name

def get_preview_data(con, table_name, num_rows = 5):
    """
    Get preview data for a given table.

    Args:
        con (Connection): The DuckDB connection object.
        table_name (str): The name of the table to preview.
        num_rows (int): The number of rows to preview.

    Returns:
        df(pandas.DataFrame): A DataFrame containing the preview data.
    """

    #get first 5 rows of the table
    query = f"SELECT * FROM {table_name} LIMIT {num_rows}"
    df = con.execute(query).fetchdf()
    #reset index to start from 1
    df.index = range(1, len(df) + 1)
    return df

def data_preview(num_rows = 5):
    """
    Display data preview for each table.

    Args:
        num_rows (int): The number of rows to preview.

    Returns:
        st.dataframe(preview_df): The preview of the selected table.
    """
    with st.expander("Data Preview", expanded=False):
        tab_prev=st.selectbox('Select Table:',st.session_state.tables.keys())
        preview_df = get_preview_data(st.session_state.con, tab_prev,num_rows)
        return st.dataframe(preview_df)
        #tabs = st.tabs(list(st.session_state.tables.keys()))
        #for i, tab in enumerate(tabs):
        #    with tab:
        #        table_name = list(st.session_state.tables.keys())[i]
        #        preview_df = get_preview_data(st.session_state.con, table_name)
        #        st.dataframe(preview_df)

def get_query():
    """
    Function to get user input for SQL query and execute it.

    Returns:
        st.session_state.query_result(pd.DataFrame): The result of the executed query, or None if no query was entered.
    """
    st.subheader("Query Data")
    sql_query = st.text_area("Enter your SQL query:", height=100, key="sql_input")

    if st.button("Run Query"):
        if sql_query:
            try:
                #run query
                st.session_state.query_result=None
                st.session_state.edited_df=None
                result_df = run_query(st.session_state.con, sql_query)
                # Reset index to start from 1 for query results
                result_df.index = range(1, len(result_df) + 1)
                st.session_state.query_result = result_df
                st.success("Query executed successfully!")
                return st.session_state.query_result
            #catch exception of wrong table name and update command
            except Exception as e:
                if "Catalog Error: Table with name" in str(e):
                    st.error("Table not existing. Please check table names in your query.")
                elif "Can only update base table" in str(e):
                    st.error("Update not available. Please consider a different select statement and the edit mode.")
                else:
                    st.error(f"Error executing query: {str(e)}")
        else:
            st.warning("Please enter a SQL query.")

def run_query(con, sql_query):
    """
    Executes the given SQL query on the provided connection object and returns the result.

    Args:
        con (connection): The connection object to the database.
        sql_query (str): The SQL query to be executed.

    Returns:
        result(pd.DataFrame): The result of the SQL query as a DataFrame.
    """

    #execute sql query
    result = con.execute(sql_query).fetchdf()
    return result

def query_result():
    """
    Displays the query result and allows for editing and exporting of data.

    Returns:
        st.session_state.query_result,st.session_state.export_df(Tuple): A tuple containing the query result dataframe and the export dataframe.
    """
    st.subheader("Query Result")
    #add a toggle for edit mode
    edit_mode = st.toggle("Edit Mode")
    #manage the edit mode
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

    return st.session_state.query_result,st.session_state.export_df

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
            if selected_format in ['csv','txt']:
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

                file_content = df_to_file(st.session_state.export_df, selected_format,
                                        sep=delimiter, quoting=quoting_options[quoting],header=head)
            #manage xlsx download
            elif selected_format =='xlsx':
                header = st.selectbox("Header:",("Y", "N"))
                head=True if header == 'Y' else False
                file_content = df_to_file(st.session_state.export_df, selected_format,header=head)
            #manage other file download
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

    #create the buffer
    buffer = BytesIO()

    try:
        #manage csv and txt files
        if file_format in ['csv','txt']:
            try:
                df.to_csv(buffer, index=False, **kwargs)
            #Catch specific CSV writing errors
            except csv.Error as e:
                if "need to escape" in str(e):
                    st.warning("Special character found in the data.  \nPlease select a different quoting option.")
                else:
                    raise ValueError(f"{file_format} writing error: {e}")
        #manage xlsx files
        elif file_format == 'xlsx':
            df.to_excel(buffer, index=False, engine='openpyxl', **kwargs)
        #manage json files
        elif file_format == 'json':
            df.to_json(buffer, orient='records', **kwargs)
        #manage parquet files
        elif file_format == 'parquet':
            df.to_parquet(buffer, index=False, engine='pyarrow', **kwargs)
        #manage xml files
        elif file_format == 'xml':
            df.to_xml(buffer, index=False, **kwargs)
        #manage avro files
        elif file_format == 'avro':
            pdx.to_avro(buffer, df)
        #get unsupported files error
        else:
            raise ValueError(f"Unsupported file format: {file_format}")
    #catch errors for df to file conversion
    except Exception as e:
        st.error(e)
        st.warning(f"{file_format} export not available for your data.  \nPlease select a different format.")

    #return the buffer
    buffer.seek(0)
    return buffer.getvalue()

def main():
    """
    The main function of the DataQuery application.

    This function handles the main logic of the application, including file uploading, data loading, data preview, SQL query execution, and result visualization.
    It collects files settings for csv, txt and xlsx files.
    It also provides options for editing and downloading query results.

    Returns:
        None
    """

    #set page config
    st.set_page_config(page_title='DataQuery', page_icon=':o:', layout="centered")
    #set title and subheader
    st.title("DataQuery")
    st.subheader("Preview, query, edit and export data files")

    #supported file extensions
    file_ext=['avro','csv', 'json','parquet','txt', 'xlsx', 'xml','zip']
    #number of rows read for preview
    num_rows=5

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
        st.session_state.export_df=None

    #files upload
    upload_files(file_ext)

    #if files uploaded
    if st.session_state.uploaded_files:
        #load files to db
        files_to_db(file_ext)

        #if tables created
        if st.session_state.tables:
            # Display data preview for each table
            data_preview(num_rows=num_rows)
            #provide sql query section
            get_query()

        #if query result
        if st.session_state.query_result is not None:
            #display query result
            query_result()
                
            #display data download section
            data_download(file_ext)
    else:
        #reset session state variables
        st.session_state.tables = {}
        st.session_state.query_result = None
        st.session_state.export_df= None
        st.session_state.uploaded_files=[]
        st.session_state.edited_df = None

if __name__ == "__main__":
    main()