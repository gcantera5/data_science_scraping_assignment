from bs4 import BeautifulSoup
import requests
import sqlite3
import pandas as pd

### IEX TRADING API METHODS ###
IEX_TRADING_URL = "https://cloud.iexapis.com/stable/stock/"

### YAHOO FINANCE SCRAPING
MOST_ACTIVE_STOCKS_URL = "https://cs1951a-s21-brown.github.io/resources/stocks_scraping_2021.html"

### Register at IEX to receive your unique token
TOKEN = ''

# Using BeautifulSoup and requests to collect data required for the assignment.
html = requests.get(MOST_ACTIVE_STOCKS_URL)
soup = BeautifulSoup(html.text, 'html.parser')

# Used the inspect feature of the website to find it's HTML contents
table = soup.find('table', class_='genTbl closedTbl elpTbl elp25 crossRatesTbl')

# Defining the dataframe
df = pd.DataFrame(columns=['Name', 'Symbol', 'Price', 'Percentage_Change', 'Volume', 'HQ_State'])

for table_row in table.tbody.find_all('tr'): 
    columns = table_row.find_all('td')
    if(columns != []):
        name = columns[1].text.strip()
        symbol = columns[2].text.strip()
        price = columns[3].text.replace(",", "")
        percent_change = float(columns[5].text.strip().rstrip("%"))
        volume = columns[6].text.strip()
        hq_state = str(columns[7].text.strip()).lower()
        # The following removes the abbreviation for thousands and millions (K and M) and returns its full numerical form
        place_values = []
        if volume.endswith("K"):
            volume = volume.replace("K", "")
            place_values.append(int(float(volume.split()[0]) * 10**3))
            volume = place_values[0]
            
        elif volume.endswith("M"):
            volume = volume.replace("M", "")
            place_values.append(int(float(volume.split()[0]) * 10**6))
            volume = place_values[0]

        float_price = float(price)

        # Appending to dataframe
        df = df.append({'Name' : name, 
                        'Symbol' : symbol,
                        'Price' : float_price,
                        'Percentage_Change' : percent_change,
                        'Volume' : volume,
                        'HQ_State' : hq_state},
                        ignore_index = True)

# Use IEX trading API to collect sector and previous pricing data.
avg_dataframe = pd.DataFrame(columns=['Symbol',  'Average_Price'])
closing_price_dataframe = pd.DataFrame(columns=['Symbol', 'Closing_Price'])

for symbol in (df.loc[:,"Symbol"]):
    # Responsible for requesting the data with the average closing price of one month
    # Set the parameter “chartCloseOnly” to True when requesting the chart endpoint to avoid reaching your API call limit
    one_month_request = requests.get(IEX_TRADING_URL + symbol + '/chart/1m', params={"token": "pk_f6e01d569ea1490c8006fcfcc3494fa9", "chartCloseOnly": True})

    # Responsible for requesting the data with the closing price on January 20th, 2023
    # Set parameter “chartByDay” to True to pinpoint the close price to one value
    jan_20th_request = requests.get(IEX_TRADING_URL + symbol + '/chart/20230120', params={"token": "pk_f6e01d569ea1490c8006fcfcc3494fa9", "chartCloseOnly": True, "chartByDay": True})

    # IEX Trading API does not have data on some stocks listed on investing.com that are listed on major stock exchange.
    # To avoid getting a 404 status code from IEX Trading API, I disregard stocks from investing.com if they are not present in the IEX Trading AP
    if (one_month_request.status_code != 404):
        one_month_request_Json = one_month_request.json()
        jan_20th_request_Json = jan_20th_request.json()
        total = 0
        counter = 0
        if (len(one_month_request_Json) != 0):
            for i in range(len(one_month_request_Json)):
                counter += 1
                total += one_month_request_Json[i].get('close')
                calc = total/counter
            avg_dataframe = avg_dataframe.append({'Symbol': symbol, 'Average_Price': calc}, ignore_index=True)
        
        if (len(jan_20th_request_Json) != 0):
            for j in range(len(jan_20th_request_Json)):
                if (jan_20th_request_Json[j].get('date') == '2023-01-20'):
                    price = jan_20th_request_Json[j].get('close')
                    closing_price_dataframe = closing_price_dataframe.append({'Symbol': symbol, 'Closing_Price': price}, ignore_index=True)

# Create connection to database
# Make sure you have the right path to data.db, in case you have any connection issues
conn = sqlite3.connect('data.db')
c = conn.cursor()

# Delete tables if they exist
c.execute('DROP TABLE IF EXISTS "companies";')
c.execute('DROP TABLE IF EXISTS "quotes";')

# Create tables in the database (comapnies and symbol) and add data to it.
c.execute('CREATE TABLE companies(symbol text not null, name text not null, location text not null, PRIMARY KEY (symbol))')
conn.commit()
c.execute('CREATE TABLE quotes(symbol text not null, prev_close int, price float, avg_price int, volume int, change_pct int, FOREIGN KEY (symbol) REFERENCES companies(symbol))')
conn.commit()

# Retrieving data information
df_symbol = df.get('Symbol') 
df_name = df.get('Name')
df_price = df.get('Price') 
df_percent_change = df.get('Percentage_Change') 
df_volume = df.get('Volume') 
df_hq_state = df.get('HQ_State')
avg_symbol = avg_dataframe.get('Symbol') 
avg_data = avg_dataframe.get('Average_Price')
cd_symbol = closing_price_dataframe.get('Symbol') 
cd_prev_close = closing_price_dataframe.get('Closing_Price') 

# Adding data into companies table
for i in range(len(df)):
    c.execute('INSERT INTO companies VALUES (?, ?, ? )', (df_symbol[i], df_name[i], df_hq_state[i]))
    conn.commit()

for j in range(len(df)):

    for k in range(len(avg_dataframe)):
        if df_symbol[j] == avg_symbol[k]:
            average_price = avg_data[k]
            break
        else:
            average_price = None

    for l in range(len(closing_price_dataframe)):
        if df_symbol[j] == cd_symbol[l]:
            closing_price = cd_prev_close[l]
            break
        else:
            closing_price = None

    # Adding data into quotes table
    if (closing_price != None and average_price != None):
        c.execute('INSERT INTO quotes VALUES (?, ?, ?, ?, ?, ? )', (df_symbol[j], closing_price, float(df_price[j]), average_price, df_volume[j], df_percent_change[j]))
        conn.commit()

    # Delete data from companies table if there isn't relevant data
    if (closing_price == None or average_price == None):
        c.execute('DELETE FROM companies WHERE symbol = (?)', [df_symbol[j]])
        conn.commit()

conn.commit()