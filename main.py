import pandas as pd
import numpy  as np
import os
import datetime as dt


folder = r'C:\Users\Weili\Desktop\DailyPaperTrades'
unrecorded_folder = os.path.join(folder, 'Unrecorded')
master_file = os.path.join(folder, 'Master - TradeRecords.xlsx')


#Procedure created to read clean data from each file
def read_clean_data(file, output = 'transactions'):
    #import the file as a dataframe
    df = pd.read_csv(file)
    
    #Find the rows and columns to drop and only keep relevant information
    beg_row = df[df.apply(lambda x: (x == 'DATE').any(), axis = 1)].index[0]
    stop_row = df[df.apply(lambda x: (x == 'Futures Statements').any(), axis = 1)].index[0]
    
    df = df.iloc[beg_row : stop_row].dropna(how = 'all', axis = 1)
    df.columns = ['Date', 'Time', 'Type', 'OrderID', 'Description', 'TradingFees'
                        , 'Commissions', 'Amount', 'EndingBalance']
    df.drop(df[~df['Date'].apply(lambda x: True if isinstance(x, str) 
                                    and any([letter.isdigit() for letter in x]) else False)].index
                                     , inplace = True)
    
     #Filter to only relevant rows based on user input
    if output == 'transactions':
        df = df.iloc[np.where(df['Type'] == 'TRD')]
    elif output == 'balance':
        df = df.iloc[np.where(df['Type'] == 'BAL')]
    
    #Reset the index
    df.reset_index(drop = True, inplace = True)
    df.rename_axis(None, axis = 1, inplace = True)
    
    #Format the columns accordingly
    df['Date'] = df['Date'].apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
    df['Time'] = df['Time'].apply(lambda x: dt.datetime.strptime(x, '%H:%M:%S').time())
    df['Time'] = df.apply(lambda x: dt.datetime.combine(x['Date'], x['Time']), axis = 1)
    df['Amount'] = df['Amount'].apply(lambda x: float(x.replace(',','')))
    df['EndingBalance'] = df['EndingBalance'].apply(lambda x: float(x.replace(',','')))
    
    return df

#Procedure using read_clean_data to get data from each file in a folder
def read_unrecorded_files(filepath):
    #ObjectiveRead the files from the folder containing records that haven't been added
    #Delete the dataframe if current exists in system. If not, pass to do the procedure
    try:
        del unrecorded_df
    except (Exception, BaseException) as e:
        pass
    #Future Note: Can use (dirpath,dirname, filenames) = next(os.walk(unrecorded_folder)) in the future
    for file in os.listdir(filepath):
        if file.endswith(('.csv','xlsx')): 
            file_output = read_clean_data(os.path.join(filepath, file))
            if 'unrecorded_df' in locals():
                unrecorded_df = unrecorded_df.append(file_output)
            else:
                unrecorded_df = file_output
    return unrecorded_df

def add_key_info(df):
    #Part 1 - Objective: Adding new columns with key information
    df['StartingBalance'] = df.apply(lambda x: abs(x['Amount']) + x['EndingBalance'] 
                                                     if x['Amount'] < 0\
                            else x['EndingBalance'] - x['Amount'] if x['Amount'] >= 0\
                            else np.nan, axis = 1)
    df['Utilization'] = df.apply(lambda x: abs(x['Amount']) / x['StartingBalance']
                            if x['Amount'] < 0 else 0, axis = 1)
    df['Hour'] = df['Time'].dt.hour
    df['Direction'] = df['Description'].apply(lambda x: 
                    'Buy' if x.split(' ')[0] == 'BOT' 
                    else 'Sell' if x.split(' ')[0] == 'SOLD' else 'N/A' )
    df['Size'] = df['Description'].apply(lambda x: 
                     int(x.split(' ')[1].replace(',','')))
    df['Ticker'] = df['Description'].apply(lambda x: x.split(' ')[2])
    df['Price'] = df['Description'].apply(lambda x: float(x.split(' ')[-1].strip('@')))
    
    #Sort the table by ticker for position calculation
    df.sort_values(by = ['Ticker', 'Time'], inplace = True)
    df.reset_index(drop = True, inplace = True)
    
    #Part 2 - Objective: Adds 1)EndPosition and 2)OrderAction to columns
    rolling_total = 0
    last_ticker = ''
    ticker_round = 1
    for row in df.itertuples():
        #If 1)first in the record or 2)onto a new ticker symbol
        if not last_ticker or row.Ticker != last_ticker:
            ticker_round = 1
            df.at[row.Index, 'EndingPosition'] = row.Size
            df.at[row.Index, 'OrderAction'] = 'Open'
            df.at[row.Index, 'Transaction'] = f'{row.Ticker}-Trade{ticker_round}'
            rolling_total = row.Size
            
        #If 1)Remaining same symbol
        else:
            #1a.Same symbol but a new trade
            if 'Close' in df.at[row.Index - 1, 'OrderAction']:
                df.at[row.Index, 'EndingPosition'] = rolling_total + row.Size
                df.at[row.Index, 'OrderAction'] = 'Open'
                df.at[row.Index, 'Transaction'] = f'{row.Ticker}-Trade{ticker_round + 1}'
                ticker_round += 1
            #1b. Same symbol and same trade    
            else:
                df.at[row.Index, 'EndingPosition'] = rolling_total + row.Size
                df.at[row.Index, 'OrderAction'] = 'Close' if (rolling_total + row.Size == 0)\
                                            else 'Increase' if row.Size > 0\
                                            else 'Decrease' if row.Size < 0\
                                            else np.NaN
                df.at[row.Index, 'Transaction'] = f'{row.Ticker}-Trade{ticker_round}'
            rolling_total += row.Size

        last_ticker = row.Ticker

    return df

def add_to_destination(target_df, destination_file, destination_sheet
                       , include_index = False, include_header = False):
    #Get starting row, and starting column if needed, for the new data to append to
    matrix_adj = {'row': 1, 'column': 1}
    current_matrix = pd.read_excel(destination_file, sheet_name = destination_sheet, header = 0).shape
    
    #List apprehension to assign starting row and starting column values
    #start_row, start_col = tuple(current + adj for current, adj in zip(current_matrix, matrix_adj))
    if current_matrix[0] == 0:
        start_row = 0
        include_header = True
    else:
        start_row = current_matrix[0] + matrix_adj.get('row')

    #Append to sheet with dynamic starting row and column
    try:
        with pd.ExcelWriter(destination_file
                            , engine = 'openpyxl', mode='a') as writer:
            #book.worksheets spits out exisiting worksheets with 'title' property
            #need this line for ExcelWriter to recognized all exisiting tabs in writer.sheets for appending
            writer.sheets = {ws.title : ws for ws in writer.book.worksheets}
            target_df.to_excel(writer, sheet_name = destination_sheet
                                   , index = include_index, header = include_header
                                   , startrow = start_row)
    except (Exception, BaseException) as e:
        raise e
     
gb = nba.groupby(['Date','Transaction']).agg(
    StartingBalance = ('StartingBalance', 'first') #Replaceble with lambda x: x.iloc[0] or [-1]
    , EndingBalance = ('EndingBalance', 'last')
    , Profit = ('Amount', 'sum')
    , EntryTime = ('Time', 'min')
    , ExitTime = ('Time', 'max')
    , TransactionLegs = ('OrderID', 'count')
).reset_index()

gb['ProfitPct'] = round(gb['Profit']/gb['StartingBalance'], 3)
gb['Duration'] = gb['ExitTime'] - gb['EntryTime']
gb['Result'] = gb.apply(lambda x: 'Win' if x['Profit'] > 0 
                       else 'Loss' if x['Profit'] < 0
                       else 'Breakeven', axis = 1)
