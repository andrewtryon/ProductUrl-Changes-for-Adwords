from dotenv import load_dotenv
load_dotenv()
import os
import json
import logging
import logzero
import numpy as np
import pandas as pd
import pyodbc
import subprocess
import requests
#from sqlalchemy import create_engine
#from sqlalchemy.pool import StaticPool
from subprocess import Popen
import json
import pickle
from datetime import datetime, timedelta
from pandas.io.json import json_normalize

def makeWrikeTask (title = "New Pricing Task", description = "No Description Provided", status = "Active", assignees = "KUAAY4PZ", folderid = "IEAAJKV3I4JBAOZD"):
    url = "https://www.wrike.com/api/v4/folders/" + folderid + "/tasks"
    querystring = {
        'title':title,
        'description':description,
        'status':status,
        'responsibles':assignees
        } 
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
        }        
    response = requests.request("POST", url, headers=headers, params=querystring)
    return response

def attachWrikeTask (attachmentpath, taskid):
    url = "https://www.wrike.com/api/v4/tasks/" + taskid + "/attachments"
    headers = {
        'Authorization': 'bearer TOKEN'.replace('TOKEN',os.environ.get(r"WRIKE_TOKEN"))
    }

    files = {
        'X-File-Name': (attachmentpath, open(attachmentpath, 'rb')),
    }

    response = requests.post(url, headers=headers, files=files)
    print(response)
    return response       


def flatten_json(nested_json, exclude=['']):
    out = {}
    def flatten(x, name='', exclude=exclude):
        if type(x) is dict:
            for a in x:
                if a not in exclude:
                    flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                if '_products' in name:
                    out[name[:-1]] = x
                elif a not in exclude: 
                    flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    flatten(nested_json)
    return out    

def construct_qarl_sql(table, row, code, sql_type = 'update'):   
    no_quote_columns = ['ClearanceFlag','Weight','ShipWeight','ShipLength','ShipWidth','ShipHeight']
    row_dict = row.dropna().to_dict()
    if table == 'ProductInfo':
        row_dict['DateUpdated'] = datetime.today().strftime("%m/%d/%Y")
    if sql_type == 'update':
        del row_dict['ItemCode']
        #sql_set_data = ", ".join([k + " = '" + v + "'" for k,v in row_dict.items()])
        sql_set_data = ", ".join([k + " = " + str(v) if k in no_quote_columns else k + " = '" + v.replace("'","''") + "'" for k,v in row_dict.items()])
        sql = """UPDATE target_table 
                SET data_set
                WHERE ItemCode = 'row_ItemCode'""".replace('target_table',table).replace('row_ItemCode',code).replace('data_set',sql_set_data)
    elif sql_type =='add':
        row_keys = ",".join([k for k in row_dict.keys()])
        row_data = ",".join([str(row_dict[k]) if k in no_quote_columns else "'" + row_dict[k] + "'" for k in row_dict.keys()])
        sql = """INSERT INTO target_table (table_columns) 
                VALUES (table_values)""".replace('target_table',table).replace('table_columns',row_keys).replace('table_values',row_data)   
    return sql

def make_json_attribute_data_nest(row, column_name, unit, currency):
    if row[column_name] is None or row[column_name] is np.nan or str(row[column_name]) == 'nan':
        # or str(row[column_name]) == ''
        row[column_name] = np.nan  
    elif type(row[column_name]) != list:
        if isinstance(row[column_name], bool):
            d = row[column_name]
        elif not isinstance(row[column_name], str):
            d = str(row[column_name]).encode().decode()
        else:
            d = row[column_name].encode().decode()
        if unit is not None and currency is None:
            if row[column_name] == '':
                row[column_name] = np.nan
                return row
            else:
                d = np.array({"amount":d,"unit":unit}).tolist()
        elif unit is None and currency is not None:
            d = [np.array({"amount":d,"currency":currency}).tolist()]
        d = {"data":d,"locale":None,"scope":None}
        row[column_name] = [d]
    return row    

if __name__ == '__main__':

    current_run_time = datetime.today()# - timedelta(hours=12)
    print(current_run_time)

    # Uncomment Below to establish a new pickle for last runtime
    # current_run_time = datetime.today() - timedelta(hours=48)
    # with open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-tron_runtime.p', 'wb') as f:
    #     pickle.dump(current_run_time, f)
    # exit()

    last_run_time = pickle.load(open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-tron_runtime.p','rb')) - timedelta(hours=.1)
    
    # try:
    #     last_run_time = pickle.load(open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-tron_runtime.p','rb')) - timedelta(hours=.1)
    # except:
    #     last_run_time = datetime.today() - timedelta(hours=24)
    print(last_run_time)

    logzero.loglevel(logging.WARN)

    try:
        from akeneo_api_client.client import Client
    except ModuleNotFoundError as e:
        import sys
        sys.path.append("..")
        from akeneo_api_client.client import Client

    AKENEO_CLIENT_ID = os.environ.get("AKENEO_CLIENT_ID")
    AKENEO_SECRET = os.environ.get("AKENEO_SECRET")
    AKENEO_USERNAME = os.environ.get("AKENEO_USERNAME")
    AKENEO_PASSWORD = os.environ.get("AKENEO_PASSWORD")
    AKENEO_BASE_URL = os.environ.get("AKENEO_BASE_URL")

    # Why are the keys in the .env, but then also in the file directly?

    akeneo = Client(AKENEO_BASE_URL, AKENEO_CLIENT_ID,
                    AKENEO_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)

    qarl_General_table = ['PriceListDescription','ProductLine','ProductType','ShipWeight','CatalogNumber','CountryofOrigin','RFQEnabled','DisplayName','Condition','Height','Length','Weight','Width']
    qarl_Google_table = ['GoogleId','GoogleProductCategory','GoogleProductType']
    qarl_ProductInfo_table = ['Header','Title150','Title70','Description','DatasheetUrl','ProductUrl','Accessories','AdditionalImages','BrochureUrl','Category1','Category2','Category3','Components','Features','InformationSource','Keywords','ImageUrl','PersonUpdated']    

    #need to remove this at some point
    qarl_association_cols = {
    }

    #These are the k:v mapping of System to the Akeneo equive
    # qarl_cols = {
    #     "DisplayName" : "DisplayName",
    #     "Header" : "Header",
    #     "ProductUrl" : "ProductUrl",
    #     "Title150" : "Title150"    
    # }

    # sage_cols = {
    #     "ItemCodeDesc" : "Header",
    #     "UDF_PRODUCT_NAME_150" : "Title150",
    #     "UDF_PRODUCT_NAME_100" : "Title70",
    #     "UDF_PRODUCT_NAME_70" : "Title70",
    #     "Weight" : "product_weight",        
    #     "UDF_WEB_DISPLAY_MODEL" : "DisplayName"
    # }
     
    #akeneo_att_list = list(qarl_cols.values())# + list(sage_cols.values())
    akeneo_att_list = ['DisplayName','Header','ProductUrl','ProductUrl_Delta','Title150']# + list(sage_cols.values())
    #akeneo_att_list = list(set(akeneo_att_list)) #removes dupes
    akeneo_att_string = ','.join(akeneo_att_list) #+ "ProductUrl_Delta" #these fellas toggle whether or not data needs to be synced back to systems
    

    query_run_time = last_run_time.strftime("%Y-%m-%d %H:%M:%S") #Time/Date formatting for Akeneo API
    
    searchparams = """
    {
        "limit": 100,
        "scope": "ecommerce",
        "attributes": "search_atts",
        "with_count": true,        
        "search": {
            "updated":[{"operator":">","value":"since_date"}],
            "ProductUrl":[{"operator":"NOT EMPTY"}]
        }
    }
    """.replace('since_date',query_run_time).replace('search_atts',akeneo_att_string)

    #make JSON for API call
    aksearchparam = json.loads(searchparams)
    #Get API object to iternate through
    result = akeneo.products.fetch_list(aksearchparam)

    #setting up the dataframe to be filled   
    pandaObject = pd.DataFrame(data=None)  

    #loopy toogles
    go_on = True
    count = 0
    #for i in range(1,3):  #this is for testing ;)  
    while go_on:
        count += 1
        try:
            print(str(count) + ": normalizing")                        
            page = result.get_page_items()

            #flatten a page JSON response into a datafarme (excludes the JSON fields that are contained in the list below)
            pagedf = pd.DataFrame([flatten_json(x,['scope','locale','currency','unit','categories']) for x in page])

            #below code cleans up column headers that exploded during the flattening process
            pagedf.columns = pagedf.columns.str.replace('values_','')
            pagedf.columns = pagedf.columns.str.replace('_0','')
            pagedf.columns = pagedf.columns.str.replace('_data','')
            pagedf.columns = pagedf.columns.str.replace('_amount','')
            pagedf.columns = pagedf.columns.str.replace('associations_','')
            pagedf.columns = pagedf.columns.str.replace('_products','')

            #This code would be used if you only wanted certain columns...since we defined which attributes to grab, we don't need this
            #pagedf.drop(pagedf.columns.difference(only_wanted_certain_columns_list), 1, inplace=True)
            
            #This appends each 'Page' from the the API to the              
            pandaObject = pandaObject.append(pagedf, sort=False)
        except:
            #...means we reached the end of the API pagination
            go_on = False
            break
        go_on = result.fetch_next_page()

    if pandaObject.shape[0] == 0:
        #this checks if anything was returned from the API (if nothing has been updated in Akeneo since last run...this should happen[no reason to record this runtime])
        print("nothing to sync...i guess")
        pandaObject.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\no-data.csv')
        exit()

    #Rename identifier to ItemCode
    pandaObject = pandaObject.rename(columns={"identifier": "ItemCode"})
    
    #Determine which of the 'Updated in Akeneo since last run actually need to be synced
    #Anything with this toggle 'AkeneoSyncSupport' will be synced
    #Changes to information source will also trigger
    #at end of script, ProductUrl_Delta will be overwritten with current ProductUrl
    #pandaObject.loc[(pandaObject['ItemCode'] == '000060'),'ProductUrl_Delta'] = "TEST MAKE ME ADWORDS PLRES ^-^"   
    pandaObject = pandaObject.set_index('ItemCode', drop=True)    
    adwordsdf = pandaObject.loc[pandaObject['ProductUrl'] != pandaObject['ProductUrl_Delta']]#.rename(columns={"identifier": "ItemCode"})#.set_index('ItemCode', drop=True)



    #viewing and backup
    print(adwordsdf)
    adwordsdf.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last-akene-o-tronAPIpull.csv')

    if adwordsdf.shape[0] > 0:       
        #adwords for kysenia
        #Need to analyze changes to productURL ...which I have pickled here (gets updated as Product URLS are updated)
        # last_qarl_product_url_df = pd.read_pickle('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\qarl_product_url_df.p')
        # last_qarl_product_url_df = last_qarl_product_url_df.set_index('ItemCode', drop=True)

        #Make df of most recent Product Urls that came from the API, and attach the old product URLs
        #adwordsdf = pandaObject.loc[:, ['ItemCode','ProductUrl']].set_index('ItemCode', drop=True).join(last_qarl_product_url_df, rsuffix = '-Old')#
        #Kysenia doesn't care about things without URLs
        #adwordsdf = adwordsdf.dropna(subset=['ProductUrl'])
        #If the New Url doesn't match the old...it must be some sort of 'URL Change' (same functionality as current QARL return task)
        adwordsdf.loc[:,'UrlStatus'] = 'URL Change' 
        #If old product URL is null or blank.. this  much a 'New' URL (same functionality as current QARL return task)
        adwordsdf.loc[(adwordsdf['ProductUrl_Delta'].isnull()), 'UrlStatus'] = 'New' 
        adwordsdf.loc[(adwordsdf['ProductUrl_Delta'] == ''), 'UrlStatus'] = 'New' 
        adwordsdf.loc[(adwordsdf['ProductUrl_Delta'] == 'I AM NOT ALIVE'), 'UrlStatus'] = 'New' 

        #Not sure this is need...but just in case
        #adwordsdf = adwordsdf.dropna(subset=['UrlStatus'])
        
        #Update with 'Header' information (same functionality as current QARL return task)
        # adwordsdf['Header'] = np.nan
        # adwordsdf.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\adwordsdf.csv')
        # #adwordsdf.update(workingdf)
        # adwordsdf.update(workingdf.loc[~workingdf.index.duplicated(), :])
        #adwordsdf.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\adwordsdf.csv')
        #adwordsdf['Header'] = pandaObject['Header']
        #adwordsdf.to_csv('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\adwordsdf2.csv')

        #If we have product URL changes as a result of a sync...we better grab some attention data to make Kysenia's life better (this is likely data that should be helpful while making adwords campaigns)
        if adwordsdf.shape[0] > 0:
            item_count = adwordsdf.shape[0]

            conn_str = os.environ.get(r"sage_conn_str").replace("UID=;","UID=" + os.environ.get(r"sage_login") + ";").replace("PWD=;","PWD=" + os.environ.get(r"sage_pw") + ";") 

            #Establish sage connection
            print('Connecting to Sage')
            cnxn = pyodbc.connect(conn_str, autocommit=True)    
            
            #SQL Sage data into dataframe
            sql = """
                SELECT 
                    CI_Item.ItemCode, 
                    CI_Item.UDF_WEB_DISPLAY_MODEL_NUMBER AS 'Display Model',
                    CI_Item.UDF_CATALOG_NO AS 'Catalog No.',
                    CI_Item.SuggestedRetailPrice AS 'MSRP', 
                    CI_Item.UDF_MAP_PRICE AS 'MAP', 
                    CI_Item.StandardUnitPrice AS 'SalePrice', 
                    CI_Item.StandardUnitCost AS 'Cost', 
                    CI_Item.LastSoldDate, 
                    CI_Item.LastReceiptDate,
                    CI_Item.DateCreated,
                    IM_ItemWarehouse.TotalWarehouseValue AS 'WarehouseValue', 
                    IM_ItemWarehouse.QuantityOnHand AS 'QtyOH',
                    IM_ItemWarehouse.QuantityOnPurchaseOrder AS 'QtyPO',
                    IM_ItemWarehouse.QuantityOnSalesOrder AS 'QtySO',
                    IM_ItemWarehouse.QuantityOnBackOrder AS 'QtyBO',
                    IM_ItemWarehouse.ReorderPointQty,
                    CI_Item.InactiveItem
                FROM 
                    CI_Item CI_Item, 
                    IM_ItemWarehouse IM_ItemWarehouse
                WHERE 
                    IM_ItemWarehouse.WarehouseCode = '000' AND 
                    CI_Item.ItemCode = IM_ItemWarehouse.ItemCode
            """
            #Execute SQL
            print('Retreiving Sage data')
            SageDF = pd.read_sql(sql,cnxn).set_index('ItemCode')            

            #Join data with our adwordsdf
            adwordsdf = adwordsdf.drop(adwordsdf.columns.difference(['ProductUrl_Delta','ProductUrl','Header','Title150','UrlStatus']), 1,)
            adwordsdf = adwordsdf.join(SageDF)
            adwordsdf = adwordsdf.rename(columns={"ProductUrl_Delta": "ProductUrl-Old"})
            #pandaObject = pandaObject.drop(pandaObject.columns.difference(['ProductUrl-Old','ProductUrl','Header']), 1,)

            #adwordsdf = adwordsdf.query("InactiveItem != 'Y'")

            if adwordsdf.query("InactiveItem != 'Y'").shape[0] > 0:

                #Save File to be sent through API
                filename = "\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\" + current_run_time.strftime("%Y-%m-%d_%Hh-%Mm") + ' Adwords_(' + str(item_count) + ').xlsx'
                adwordsdf.query("InactiveItem != 'Y'").to_excel(filename)
                assignees = '[KUAAZJ3D,KUAAY4PZ]' #Ksenyia Kris
                folderid = 'IEAAJKV3I4DLO7CM' #Qarl reimport foldser
                description = "These products have had their product urls updated. Adjustments of Google Adwords is required here." 
                response = makeWrikeTask(title = "Returned Data for Adwords - " + (current_run_time).strftime("%Y-%m-%d")+ " (" + str(item_count) + "ads)", description = description, assignees = assignees, folderid = folderid)
                response_dict = json.loads(response.text)
                print(response_dict)
                taskid = response_dict['data'][0]['id']
                print('Attaching file to ', taskid)
                attachWrikeTask(attachmentpath = filename, taskid = taskid)
                print('File attached!') #probably should have a handle for the...can't attach too big
            else:
                print("All inactive products.... I guess")
            

            #backups for next run        
            #pickle product URLs
            # last_qarl_product_url_df = pd.concat([last_qarl_product_url_df,pandaObject.loc[:, ['ItemCode', 'ProductUrl']].set_index('ItemCode')])   
            # last_qarl_product_url_df = last_qarl_product_url_df.drop_duplicates(keep='last').reset_index(drop=False)  
            # print(last_qarl_product_url_df)        
            # with open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\qarl_product_url_df.p', 'wb') as f:
            #     pickle.dump(last_qarl_product_url_df.loc[:, ['ItemCode', 'ProductUrl']], f)            

            #Data need to be sent back to akeneo to prevent these items from continusiously resyncing
            #prepping
            print(adwordsdf)
            adwordsdf = adwordsdf.reset_index(drop=False)
            adwordsdf = adwordsdf.rename(columns={"ItemCode": "identifier"})
            print(adwordsdf)
            #adwordsdf = adwordsdf.set_index('identifier')   
            adwordsdf['ProductUrl_Delta'] = adwordsdf['ProductUrl'] 
            #pandaObject['AkeneoSyncSupport'] = False
            
            #Flatten df to JSON
            valuesCols = [
                'ProductUrl_Delta'
            ]
            print(adwordsdf)
            for cols in valuesCols:
                adwordsdf = adwordsdf.apply(make_json_attribute_data_nest, column_name = cols, currency = None, unit = None, axis = 1) 
            print(adwordsdf)    
            jsonDF = (adwordsdf.groupby(['identifier'], as_index=False)
                        .apply(lambda x: x[valuesCols].dropna(axis=1).to_dict('records'))
                        .reset_index()
                        .rename(columns={'':'values'}))
            print(jsonDF)
            jsonDF.rename(columns={ jsonDF.columns[2]: "values" }, inplace = True)

            load_failure = False
            api_errors_file = open("\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\Akeneo_Sync_Data_Errors-o-tRON.csv", "w") 

            print(jsonDF)
            #Send data
            try:
                values_for_json = jsonDF.loc[:, ['identifier','values']].dropna(how='all',subset=['values']).to_dict(orient='records')   
                data_results = akeneo.products.update_create_list(values_for_json)
                print(data_results)   
            except requests.exceptions.RequestException as api_error:
                load_failure = True
                api_errors_file.write(str(api_error)) 

            api_errors_file.close()

    #Saving Last run time
    print("pickling")
    #current_run_time.to_pickle(r'\\FOT00WEB\Alt Team\Andrew\Andrews_Code\akeneolyzer\last_akene-O-nator_runtime.p')
    with open('\\\\FOT00WEB\\Alt Team\\Andrew\\Andrews_Code\\last_akene-O-tron_runtime.p', 'wb') as f:
        pickle.dump(current_run_time, f)
    f.close()               