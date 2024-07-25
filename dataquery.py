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
#import avro.schema
#from avro.datafile import DataFileReader
#from avro.io import DatumReader

def load_file(file, con):
    """
    Load a file into a dataframe and register it in a database connection.

    Parameters:
    - file: File object or path to the file.
    - con: Database connection object.

    Returns:
    - List of table names where the data was registered, or None if there was an error.

    Raises:
    - None.
    """

    #get file extension and file name
    file_extension = file.name.split('.')[-1].lower()
    file_nm=file.name.replace('.','_')

    table_names = []

    #read the file content
    try:
        #manage csv and txt files
        if file_extension in ['csv', 'txt']:
            sample = file.read(8192)
            file.seek(0)
            #detect delimiter, header and quoting settings
            try:
                dialect = csv.Sniffer().sniff(sample.decode('utf-8'),delimiters=[',',';','|','\t'])
                cs_header=csv.Sniffer().has_header(sample.decode('utf-8'))
                cs_header=0 if cs_header else None
                df = pd.read_csv(file,
                                sep=dialect.delimiter,
                                quotechar=dialect.quotechar,
                                escapechar=dialect.escapechar,
                                skipinitialspace=dialect.skipinitialspace,
                                header=st.session_state.header)
            #if failed, create one column of data with header
            except:            
                cs_header=0
                df=pd.read_csv(file,header=st.session_state.header)
            
            #if no header found rename columns from default simple integer to col_integer
            if cs_header is None:
                    df.columns = [f'col_{i+1}' for i in range(len(df.columns))]  
        
        #manage xlsx files
        elif file_extension == 'xlsx':
            xls = pd.ExcelFile(file)
            #get a df for each sheet
            dfs = {sheet_name: pd.read_excel(file, sheet_name=sheet_name) for sheet_name in xls.sheet_names}
        #manage parquet files
        elif file_extension == 'parquet':
            df = pd.read_parquet(file)
        #manage avro files
        elif file_extension == 'avro':
            df = pdx.read_avro(file, na_dtypes=True)
        #manage json files
        elif file_extension == 'json':
            json_data = pd.read_json(file)
            #Normalize nested JSON data
            df = pd.json_normalize(json_data.to_dict('records')) if isinstance(json_data, pd.DataFrame) else pd.json_normalize(json_data.to_dict())
        #manage xml files
        elif file_extension == 'xml':
            df = pd.read_xml(file)
        #get unsupported files
        else:
            st.error(f"File {file.name} not loaded. Unsupported file format.")
            return None

        #register view in db for xlsx
        if file_extension == 'xlsx':
            for sheet_name, df in dfs.items():
                table_name = register_dataframe(con, df, f"{file_nm}_{sheet_name}".lower())
                table_names.append(table_name)
        #register view in db for all other files
        else:
            table_name = register_dataframe(con, df, file_nm.lower())
            table_names.append(table_name)

        return table_names

    #catch errors working files
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
        str: The cleaned table name.
    """

    #replace spaces with underscores
    name = name.replace(' ', '')
    #remove all other special characters
    name = sub(r'[^a-zA-Z0-9_]', '', name)
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

    #execute sql query
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

    #get first 5 rows of the table
    query = f"SELECT * FROM {table_name} LIMIT {num_rows}"
    df = con.execute(query).fetchdf()
    #reset index to start from 1
    df.index = range(1, len(df) + 1)
    return df

def remove_view(con, view_name):
    """
    Safely remove a view from the DuckDB connection.

    Args:
        con (Connection): The DuckDB connection object.
        view_name (str): The name of the view to remove.
    """

    #remove view from db
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
    It also provides options for editing and downloading query results.
    """

    #set page config
    st.set_page_config(page_title='DataQuery', page_icon=':o:', layout="centered")
    #set title and subheader
    st.title("DataQuery")
    st.subheader("Preview, query, edit and export data files")

    #supported file extensions
    file_ext=['avro','csv', 'json','parquet','txt', 'xlsx', 'xml','zip']

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

    #if files uploaded
    if uploaded_files:
        loaded_tab = ""
        excluded_tab = ""
        #loop uploaded files
        for file in uploaded_files:
            #check if file already loaded
            if file.name not in [f.name for f in st.session_state.uploaded_files if f != file]:
                #check if file is a zip archive
                if file.type == "application/x-zip-compressed":
                    #extract files from zip archive
                    with zipfile.ZipFile(file) as z:
                        for zip_info in z.infolist():
                            #check if file is a directory
                            if not zip_info.is_dir():
                                _, extension = splitext(zip_info.filename)
                                #check if file extension is supported
                                if extension.lstrip('.').lower() in file_ext:
                                    with z.open(zip_info) as zf:
                                        #load file
                                        extracted_file = BytesIO(zf.read())
                                        extracted_file.name = basename(zip_info.filename)
                                        loaded_tables = load_file(extracted_file, st.session_state.con)
                                        if loaded_tables:
                                            #register tables in session state
                                            for table in loaded_tables:
                                                st.session_state.tables[table] = extracted_file.name
                                            loaded_tab += f"Loaded {file.name} - {extracted_file.name} as table(s): {', '.join(loaded_tables)}  \n"
                                #if file extension not supported
                                else:
                                    excluded_tab += f"{file.name} - {zip_info.filename} not loaded. Unsupported file format  \n"
                #if file is not a zip archive
                else:
                    #load file
                    loaded_tables = load_file(file, st.session_state.con)
                    if loaded_tables:
                        #register tables in session state
                        for table in loaded_tables:
                            st.session_state.tables[table] = file.name
                        loaded_tab += f"Loaded {file.name} as table(s): {', '.join(loaded_tables)}  \n"

        # Display success and warning messages
        if loaded_tab != "":
            st.success(loaded_tab)                    
        if excluded_tab != "":
            st.warning(excluded_tab)

        # Collapsible Data Preview
        if st.session_state.tables:
            # Display data preview for each table
            with st.expander("Data Preview", expanded=False):
                tab_prev=st.selectbox('Select Table:',st.session_state.tables.keys())
                preview_df = preview_data(st.session_state.con, tab_prev)
                st.dataframe(preview_df)
                #tabs = st.tabs(list(st.session_state.tables.keys()))
                #for i, tab in enumerate(tabs):
                #    with tab:
                #        table_name = list(st.session_state.tables.keys())[i]
                #        preview_df = preview_data(st.session_state.con, table_name)
                #        st.dataframe(preview_df)

            #SQL Query Section
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

        # Display query result and download options
        if st.session_state.query_result is not None:
            st.subheader("Query Result")
             # Add a toggle for edit mode
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
                
            #Data Download Section
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
                    st.download_button(
                        label=f"Download",
                        data=file_content,
                        file_name=file_name,
                        mime=mime_type,
                    )
    else:
        #reset session state variables
        st.session_state.tables = {}
        st.session_state.query_result = None
        st.session_state.export_df= None
        st.session_state.uploaded_files=[]
        st.session_state.edited_df = None

if __name__ == "__main__":
    main()