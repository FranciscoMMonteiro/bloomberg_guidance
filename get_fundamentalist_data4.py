# 1. Importing libraries
import sys
import os
user = os.getlogin()
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib")
sys.path.append(f"C:\\Users\\{user}\\ATMOSPHERE\\Atmosphere Capital - Gestão Geral\\Automation\\AtmLib\\atmlib")

import MongoDB
import mongo
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import xlwings as xw
import time
import subprocess
from tqdm import tqdm
import warnings
import traceback
import numpy as np


# 2. Defining functions

def get_tickers_from_bd(mdb)->list:
    """
    This function gets the list of equity bloomberg tickers that are in use.
    Basically the equity tickers our company has an interest to keep track.

    Parameters:
    ----------
    mdb : atmlib.mongo.OurMongoClient
        This parameter is the intance of the MongoDB client

    Returns:
    -------
    list : 
        List of bloomberg equity tickers that are in use in our context.
    """
    list_tickers = []
    collection = mdb.client['gestao']['asset.metadata'].find({'type':'equity','in_use':True})
    for doc in collection:
        list_tickers.append(doc['ticker']['bbg'])
    return list_tickers


def create_bql_function(bql_function:str, source:str, start_date:str, end_date:str, quarter_or_anual:str, actual_or_estimate:str, flds:str )->str:

    """
    This fuction creates a BQL query based on the parameters passed.

    Parameters:
    ----------
    bql_function : str
        Name of the function used in bloomberg to find a specific measure.
    source : str
        The source where the information comes from.
    start_date : str
        The year from when the query will begging to gather data from.
    end_date : str
        The year when the query will end to gather data from.
    quarter_or_anual : str
        Indicates the periodicity of the data. 'Q' for quarter ,  'S' for semester or 'A' for year.
    actual_or_estimate : str
        Indicates if the gathered data will be the estimate that was calculated by some source or if
        the data will be the actual realised value for the measure in question that the company achived.
        'A' being of actual and 'E' for estimate
    flds : str
        Changes the query to get diferent fields of the BLQ fuction (measurement) in question.

    Returns:
    --------
    str :
        Returns a string of the BQL functions that will be passed to excel cells.
    """
    
    bql_function = fr"dropna({bql_function}(EST_SOURCE={source},FPR=range(start='{start_date}',end='{end_date}'),FPT='{quarter_or_anual}',AE='{actual_or_estimate}')).{flds}"
    
    return bql_function

def create_bql_request(tickers_range,functions_range)-> str:
    """
    This function gets all BQL functions from a specified range in excel and creates a str unifying them all in one request.

    Parameters:
    ----------
    tickers_range : str
        A string of the cells range where the tickers will be in the excel sheet.
    tickers_range : str
        A string of the cells range where the bql functions will be in the excel sheet.

    Returns:
    -------
    str :
        Returns a string with the complete BQL function that will make the request from bloomberg.
    """
    request_str = fr"""=@BQL({tickers_range},TEXTJOIN(",",TRUE,{functions_range}))"""
    return request_str



def monta_df(df_value,df_source,source,bql_function,quarter_or_annual,actual_or_estimate)->pd.DataFrame:
    """
    This function creates a consolidated pandas dataframe from another dataframe with the values gathered
    from bloomberg,a dataframe with the source of those values, the BQL function that was used,
    the periodicity of the data and if the value is an estimated or the actual realised value of 
    the company.

    Parameters:
    ----------
    df_value : Pandas.DataFrame
        A pandas dataframe with a date column and a value column
    df_source : Pandas.DataFrame
        A pandas dataframe with the bloomberg tickers names and the sources of the information,
        in the same order they came on the df_value dataframe.
    source : 
        The source parameter that was passed to the BQL query that requested the data.
    quarter_or_annual:
        The periodicity parameter that was passed to the BQL query that requested the data.
    actual_or_estimate:
        The actual or estimate parameter that was passed to the BQL query that requested the data.

    Returns:
    -------
    Pandas.DataFrame() : Returns a pandas dataframe in a friendly format consolidating all
    the information passed to the BQL query as well as the results from the requests. 
    """
    if source == 'BROKERS_ALL':
        if df_value.empty or df_source.empty:
            return pd.DataFrame()
        df_source = df_source.reset_index()
        if df_value[df_value.columns[1]].isna().all() or df_source[df_source.columns[1]].isna().all():
            return pd.DataFrame()
        df_source.rename(columns = {df_source.columns[0]:'ticker',df_source.columns[1]:'source'}, inplace=True)
        df_source = df_source.reset_index()
        df_source.rename(columns={'index': 'idx'}, inplace=True)
        df_pivot_source = pd.pivot_table(df_source,index=['idx'], columns=['ticker'], values=['source'], aggfunc='first')
        list_dfs = []
        for col in df_pivot_source.columns:
            df_col = df_pivot_source[col]
            df_col = df_col.dropna()
            df_col = df_col.reset_index()
            df_col = df_col.drop(columns = 'idx')
            df_col.columns = df_col.columns.droplevel(0)
            list_dfs.append(df_col)
        df_source = pd.concat(list_dfs, axis=1)
        df_source = df_source.reset_index(drop=True)
        df_value.rename(columns = {df_value.columns[0]:'date'}, inplace = True)
        df_value = df_value.iloc[1:]
        df_value = df_value.reset_index(drop = True)
        df_value = df_value.rename(columns = {'DATE_VALUE':'date'})
        #try:
        #    df_value['date'] = pd.to_datetime(df_value['date'])
        #except:
        #    return pd.DataFrame()
        list_dfs = []
        for col in df_source.columns:
            #print(col)
            df_concat_i = pd.concat([df_value[['date',col]] , df_source[[col]]] , axis = 1)
            df_concat_i['ticker'] = col 
            col_value = str('VALUE')
            col_source = str('SOURCE')
            df_concat_i.columns = ['date',col_value, col_source, 'ticker']
            df_concat_i['period'] = quarter_or_annual
            df_concat_i['actual_or_estimate'] = actual_or_estimate
            df_concat_i['function'] = bql_function
            df_concat_i = df_concat_i.dropna(subset = col_value)
            #dict_dfs[col] = df_concat_i
            list_dfs.append(df_concat_i)
        df = pd.concat(list_dfs,axis=0)

        return df
    
    elif source == 'BST':
        if df_value.empty:
            return pd.DataFrame()
        if df_value[df_value.columns[1]].isna().all():
            return pd.DataFrame()
        
        df_value.rename(columns = {df_value.columns[0]:'date'}, inplace = True)
        df_value = df_value.iloc[1:]
        df_value = df_value.reset_index(drop = True)
        df_value = df_value.rename(columns = {'DATE_VALUE':'date'})

        df_value = df_value.melt(id_vars = 'date',value_vars = list(df_value.columns)[1:], var_name = 'ticker' , value_name = 'VALUE')
        df_value['SOURCE'] = 'bst_estimate'
        df_value['period'] = quarter_or_annual
        df_value['actual_or_estimate'] = actual_or_estimate
        df_value['function'] = bql_function
        df_value = df_value.dropna(subset = 'VALUE')
        df_value = df_value.dropna(subset = 'date')

        return df_value
    
def separa_lista(lst, n)->list[list]:
    """
    This fuction divides a list into subslist, n being the max number of itens inside each sublist.
    
    Parameters:
    ----------
    lst : list
        List to be separated in sublists
    n: int
        Number of separations.
    
    Returns:
    --------
    list(list):
        Returns a list of lists.

    """
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def fill_source_if_actual(row)->str:
    """
    This function fills with 'company' the 'SOURCE' column when column 'actual_or_estimate' is 'A'.

    Parameters:
    ----------
    row : pd.Series
        Rows of a dataframe

    Returns:
    -------
    str:
        Returns a str for the source field
    """
    if row[['source']].empty and row['actual_or_value'] == 'A':
        return 'company'
    
    return row['source']

def verify_BQL_request(cell)-> bool:
    """
    This function verifies if the BQL request on excel has been completed.
    If it is, it returns True, if it isn't it returns False.
    
    Parameters:
    ----------
    cell : 
        xlwings.main.Range
    Returns:
    --------
    bool:
        Returns True or False.
    """
    if cell.value == '#N/A Requesting Data...':
        return False
    else:
        return True
    

def get_df(start,end,bbg_function,tickers,source,type_period,e_or_a,fields):
    """
    This function creates a dataframe with a friendly format based on the response of the BQL query on excel.

    Parameters:
    ----------
    start : str
        The start date in string format (yyyy-mm-dd).
    end : str
        The end date in string format (yyyy-mm-dd).
    bbg_function : str
        Bloomberg function used in the bloomberg terminal or BQL queries to get companies financials.
    tickers : list
        List of tickers used to get financials of.
    source : str
        Source parameters that is passed to the bloomberg function. It defines from what vendors the data will come from. 
    type_period : str
        Type of financial period you have interest in getting. 'Q' for quarter, 'A' for annual.
    e_or_a : str
        Defines if the value will is an estimate ('E') or the actual realized value ('A').
    fields : list
        List of fields that will be retrived from each fuction passed to the BQL query.
    
    Returns:
    -------
    df : pandas.DataFrame
        Returns a pandas dataframe with a friendly format.
    """
    # If "A" (representing actual values) is passed then there is no need to look for the future.
    if e_or_a == 'A':
        end = datetime.datetime.today().strftime('%Y-%m-%d')

    # Creating workbook intance with xlwings
    wb = xw.Book()
    sht = wb.sheets[0]
    sht.activate()

    # Getting the length of the ticker list being passed in this python function,
    # so that when we paste the ticker and bbg_functions into excel we know exactly the range where
    # it beggings and ends
    init_row = 3
    tickers_len = len(tickers)
    final_range_tickers = init_row + tickers_len - 1
    range_tickers = f'$A${init_row}:$A${final_range_tickers}'
    
    # Pasting the bloomberg tickers we want data from.
    sht.range(f'A{init_row}').options(transpose=True).value = [tickers]
    time.sleep(0.5)

    # Calling the fuction that creates the bbg_fuctions_str for the BQL query
    list_bql_func = []
    for fld in fields:
        bql_function_str = create_bql_function(bbg_function,source,start,end,type_period,e_or_a,fld)
        list_bql_func.append(bql_function_str)
    # Pasting the bloomberg fuctions we want data from. 
    sht.range('B3').options(transpose=True).value = list_bql_func
    fields_len = len(fields)
    range_bql_func = f"B3:B{fields_len+2}"
    time.sleep(0.5)

    # Creating the BQL query with the functions already pasted.
    bql_query = create_bql_request(range_tickers,range_bql_func)
    # Pasting the query into an excel cell.
    sht.range('E3').formula = bql_query

    minutes_retry = 5
    # Checking every 3s if the BQL request has finished.
    # And if it takes more than 5 minutes it closes excel and tries again.
    time_zero = datetime.datetime.today()
    request_done = False
    while not request_done:
        request_done = verify_BQL_request(sht.range('E3'))
        time.sleep(3)
        time_now = datetime.datetime.today()
        delta_time = time_now - time_zero
        delta_time = delta_time.total_seconds()/60 # in minutes
        if delta_time > minutes_retry and request_done == False:
            time_zero = datetime.datetime.today()
            wb.close()
            time.sleep(60)
            wb = xw.Book()
            sht = wb.sheets[0]
            sht.activate()
            sht.range(f'A{init_row}').options(transpose=True).value = [tickers]
            time.sleep(0.5)
            list_bql_func = []
            for fld in fields:
                bql_function_str = create_bql_function(bbg_function,source,start,end,type_period,e_or_a,fld)
                list_bql_func.append(bql_function_str)
            sht.range('B3').options(transpose=True).value = list_bql_func
            # Creating the BQL query.
            bql_query = create_bql_request(range_tickers,range_bql_func)
            # Pasting the query into an excel cell.
            sht.range('E3').formula = bql_query

    time.sleep(1)

    # Changing the format of date cells.
    sht.range('F3').expand('down').number_format = 'yyyy-mm-dd'
    # Getting the data retrived from the BQL query into a dataframe.
    df = sht.range('E3').expand('right').expand('down').options(pd.DataFrame, index=False, header=True).value
    # Closing the excel workbook
    wb.close()
    time.sleep(2)

    df = df.drop_duplicates()
    for i in range(len(df.columns)-1):
        df = df.rename(columns = {df.columns[i+1]:df.columns[i+1].split('.')[-1].lower()})
        #df = df.rename(columns = {df.columns[i+1]:fields[i].lower()})
    
    
    df = df.dropna(subset = 'value')
    df = df.dropna(subset = 'period')

    # For some reason fillna wasn't working for the NaT in 'revision_date' so I did a workaround
    df['revision_date'] = df['revision_date'].astype(str)
    df['revision_date'] = df['revision_date'].str.replace('NaT','1900-01-01')
    df['revision_date'] = pd.to_datetime(df['revision_date'])
    df['measure'] = bbg_function
    df['actual_or_estimate'] = e_or_a
    df['date'] = df['period']
    df['date'] = df['date'].apply(lambda row: period_to_date(row))
    df['period'] = type_period
    df = df.rename(columns = {'firm_name':'source'})
    if source == "BST":
        df['source'] = 'bst_estimate'
    else:
        df['source'] = df.apply(fill_source_if_actual, axis =1)
    
    if not 'currency' in df.columns:
        df['currency'] = 'N/A'

    return df

def create_list_dict_upload(df):
    """
    This function creates a list of dictionaries in a BSON format to later upload to MongoDB

    Parameters:
    ----------
    df : pandas.DataFrame()
        A panda dataframe with columns : date , ID , measure , source , period , 
        actual_or_estimate , revision_date , currency , value.
    
    Returns:
    -------
    list_dict_upload : list[dict]
        Returns a list of dictionaries in BSON format for later upload to MongoDB.
    """
    # Making sure values don't take too much memory needlessly
    df['value'] = df['value'].round(6)
    # Creating a list with dictionaries.
    list_dict_upload = []
    for idx, row in df.iterrows():
        dict_i = {
            '_id':{
                'date':row['date'],
                'bbg_ticker':row['ID'],
                'measure':row['measure'],
                'source':row['source'],
                'period':row['period'],
                'actual_or_estimate':row['actual_or_estimate'],
                'revision_date':row['revision_date'],
                'currency':row['currency']
            },
            'value':row['value']
        }
        list_dict_upload.append(dict_i)
    return list_dict_upload

def upload_to_mongo(list_dict_upload):
    """
    This function uploads a list of documents to mongoDB.

    Parameters:
    -----------
    list_dict_upload : list
        List of dictionaries in a format to facilitate upload to mongoDB.

    Returns:
    --------
    True or False : Boolean
        Returns True if the upload was successful and False if it wasn't.
    """
    # Creating a mongoDB connection
    tipo_bd = 'PROD'
    mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))
    # Storing the collection where the data will be stored
    financials_collection = mdb.client['gestao']['bbg.company_financials']
    # Uploading the list of dictionaries we created.
    try:
        mongo.bulk_update(financials_collection,list_dict_upload)
        mdb.client.close()
        #print('Script was successful!')
        return True
    except Exception as e:
        traceback.print_exc()
        print(e)
        mdb.client.close()
        print("Error: Couldn't upload to DataBase.")
        return False
    
def period_to_date(period):
    year = int(period.split()[0])
    if 'A' in period:
        date = datetime.datetime(year,12,31)
    if 'Q' in period:
        quarter = int(period.split()[-1][-1:])
        month = quarter*3
        date = datetime.datetime(year,month,1)
        date = date + relativedelta(day = 31)

    return date

def main():

    # Supressing warnings
    warnings.simplefilter(action = 'ignore', category = pd.errors.PerformanceWarning)
    warnings.simplefilter(action ='ignore', category = FutureWarning)

    # Opening an excel instance for the addins to load.
    excel_path = "C:\\Program Files\\Microsoft Office\\root\\Office16\\EXCEL.EXE"
    subprocess.Popen([excel_path])
    time.sleep(15)

    # Creating a connection to our mongoDB database.
    tipo_bd = 'PROD'
    mdb = MongoDB.OurMongoClient(MongoDB.get_mongo_conn(environment=tipo_bd))

    # Getting a list of equity tickers from mongoDB.
    list_tickers = get_tickers_from_bd(mdb)
    # Closing mongoDb connection.
    mdb.client.close()
    # Sorting the list in alphabetical order.
    list_tickers = sorted(list_tickers)

    # Creating a place where the user can pass how many tickers they want
    # the script to run with, for test purposes.
    #list_tickers_slice = separa_lista(list_tickers,60)
    list_tickers_slice = [list_tickers]

    # Passing the periodicity variables that will be used in fuctions in the loop.
    periods = ['A','Q']
    # Passing the type of value variables that will be used in fuctions in the loop.
    actual_or_estimate = ['E','A']
    # Passing the BLQ fuctions that will be used for the BQLs queries in the loop.
    bql_functions = ['IS_EPS','IS_COMP_EPS_ADJUSTED','SALES_REV_TURN',
                    'IS_COMP_SALES','EBITDA','IS_COMPARABLE_EBITDA',
                    'IS_COMPARABLE_EBIT','CF_CAP_EXPEND_PRPTY_ADD','IS_OPER_INC',
                    'IS_OPERATING_EXPN','IS_TOT_OPER_EXP','GROSS_PROFIT',
                    'CB_IS_ADJUSTED_OPEX','IS_AVG_NUM_SH_FOR_EPS','IS_SH_FOR_DILUTED_EPS',
                    'CF_FREE_CASH_FLOW']
    bql_functions = bql_functions[:]
    # Passing the kind of source we want the script to get.
    #sources = ['BROKERS_ALL','BST','cmpy'] Defined it inside the loop using ifs statements
    # Passing time variables from which we want data from.
    years_lagging = 1
    start = f'{datetime.datetime.today().year + years_lagging}-01-01'
    years_ahead = 2
    end = f'{datetime.datetime.today().year + years_ahead}-12-31'
    # Passing fields variables from with we want the values from. 
    flds = ['PERIOD','FIRM_NAME','REVISION_DATE','CURRENCY','VALUE']
    
    # Creating a dictionary to store if we successfully uploaded every function to MongoDB.
    already_uploaded = {}
    for tickers in list_tickers_slice: # Use this loop if there are so many tickers that it is best to divide them
        for func in tqdm(bql_functions):
            list_dfs = []
            for e_or_a in actual_or_estimate:
                if e_or_a == 'E':
                    sources = ['BROKERS_ALL','BST','cmpy','cmpy_low','cmpy_high']
                else: # if e_or_a == 'A'
                    sources = ['cmpy']
                for source in sources:
                    for type_period in periods:
                        df = get_df(start,end,func,tickers,source,type_period, e_or_a, flds)
                        list_dfs.append(df)
            df_concat = pd.concat(list_dfs)
            list_dict_upload = create_list_dict_upload(df_concat)
            # Separating the list to upload to lists with a maximum of 1000 documents.
            list_of_list_dict_upload = separa_lista(list_dict_upload,1000)
            list_was_uploaded = []
            for list_upload in list_of_list_dict_upload:
                was_uploaded = upload_to_mongo(list_upload)
                list_was_uploaded.append(was_uploaded)
            if all(list_was_uploaded):
                already_uploaded[func] = [True]
            else:
                already_uploaded[func] = [False]
    
    print(pd.DataFrame(already_uploaded))
    

            

if __name__ == '__main__':
    # Supressing warnings
    warnings.simplefilter(action = 'ignore', category = pd.errors.PerformanceWarning)
    try:
        print('Getting company financials (both estimates and actuals) from multiple analyst sources from bloomblerg')
        time_init = datetime.datetime.today()
        main()
        time_end = datetime.datetime.today()
        run_time = time_end - time_init
        print(f'Script took {run_time} to run')
    except Exception as e:
        traceback.print_exc()
        print(e)

    input('Press ENTER to quit.')