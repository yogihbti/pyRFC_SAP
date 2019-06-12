#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyrfc import Connection
from pyrfc._exception import LogonError
from pprint import PrettyPrinter
import json
from con_config import ECP, SCP, ctx_dict
import pandas as pd
import re

#only for printing json we will remove it later
pp = PrettyPrinter(indent=4)


#JSON preprocessor which replaces variables from dictionary to json file
def preprocess_json(json_string,key_word_dict):
    string=json_string
    for key,value in key_word_dict.items():
        string=re.sub("(\{{2}\s*("+key+")\s*\}{2})",value,string)

    return string


# In[4]:


#function to take json config and convert it into function argument to be passed to RFC
def get_rfc_param(data):
    output_dict={}
    output_dict_list=[]
    output_dict['func_name']=data['RFC_NAME']
    output_dict['QUERY_TABLE']=data['QUERY_TABLE']
    output_dict['DELIMITER']=data['DELIMITER']
    output_dict['FIELDS']=data['FIELDS']
    option_list=[]
    option_multipart_list=[]
    
    for item in data['OPTIONS_PLAN']:
        if item['TYPE']=='FILE':
            #open file and add data in options
            df=pd.read_csv(item['FILE_NAME'],sep=item['FILE_DELIM'])
            column=list(set(df[item['COLUMN_NAME']]))
            
            field= item['TABLE_FIELD']
            
            for text in column:
                text_str=field+" EQ '"+ str(text) + "' "+item['CONCATINATE_VERB']
                option_list.append({'TEXT':text_str})
                
            option_list[-1]['TEXT']=option_list[-1]['TEXT'].rstrip(item['CONCATINATE_VERB'])+item['CONCATINATE_SUFFIX_VERB']
        
        elif item['TYPE']=='TEXT':
            option_list.append({'TEXT':item['TEXT']+''+item['CONCATINATE_SUFFIX_VERB']})
        
    if data['MULTIPART']=="TRUE":
        fo=data['OPTIONS_PLAN_MALTIPART']
        start=fo['SUBSET_START']
        chunk_size=fo['CHUNK_SIZE']
        if fo['TYPE']=='FILE':
            df=pd.read_csv(fo['FILE_NAME'],sep=fo['FILE_DELIM'],dtype=object)
            column_to_subset=list(set(df[fo['COLUMN_NAME']]))
            length_of_column = len(column_to_subset)
            field= fo['TABLE_FIELD']
            while start < length_of_column:
                column=column_to_subset[start:min(start+chunk_size,length_of_column)]
                temp_list=[]
                for text in column:
                    text_str=field+" EQ '"+ str(text) + "' "+fo['CONCATINATE_VERB']
                    temp_list.append({'TEXT':text_str})
                temp_list[-1]['TEXT']=temp_list[-1]['TEXT'].rstrip(fo['CONCATINATE_VERB'])+fo['CONCATINATE_SUFFIX_VERB']
                option_multipart_list.append(temp_list)
                start=start+chunk_size
                
             
        for temp_list_item in option_multipart_list:
            output_dict['OPTIONS']=option_list+temp_list_item
            output_dict_list.append(output_dict.copy())
    else:
        output_dict['OPTIONS']=option_list
        output_dict_list.append(output_dict)
    
    return(output_dict_list)


# In[5]:


def read_config_write_file(json_config_file_name,system):
    """
    Function to take config file and system as input. It calls the RFC to fetch the data
    and write it to file.
    """
    with open(json_config_file_name) as f:
        str_file=f.read()
        from con_config import ctx_dict
        #Pass the context dictionary and do the preprocessing of JSON to replace {{Keys}} with its value
        str_file=preprocess_json(str_file,ctx_dict)
        data = json.loads(str_file)

    #calling the parser for RFC params which parse the RFC param and gives the dictionary
    fun_input_list=get_rfc_param(data)
    
    #setting the row retrival strategy
    ROWS_AT_A_TIME=100000
    rowskips=0
    
    #delete the existing content of the file
    with open(data['WRITE_FILE'],mode="w") as file:
        file.close()
        
    #setting the flag if the header is written to file or not
    #it is required so that header is written only once on the top
    header_done=False

    try:
        #sap connnection is created using the system setting kept in the config file
        conn=Connection(**system)
        
        for fun_input in fun_input_list:
            #initializing the rowskip parameters again
            rowskips=0
            while True:
                #call the RFC with the desired parameters
                result = conn.call(**fun_input,
                            ROWSKIPS = rowskips, ROWCOUNT = ROWS_AT_A_TIME)

                #increment the rowskip counter to fetch next batch of data
                rowskips += ROWS_AT_A_TIME
                
                #write the data in the file
                with open(data['WRITE_FILE'], 'a') as f:
                    if not header_done:
                        header=""
                        for item in result['FIELDS']:
                            header=header+""+item['FIELDTEXT']+fun_input['DELIMITER']

                        header= header.rstrip(data['DELIMITER'])  
                        f.writelines(header+'\n')
                        header_done=True

                    for item in result['DATA']:
                        f.writelines(item['WA']+'\n')
                        
                # Stop the loop if no data or data less then the chuck size is returned by SAP 
                if len(result['DATA']) < ROWS_AT_A_TIME:
                    break

    except Exception as e:
        print(e)


# In[6]:


#Read the json and save the file
json_config_file_names=[
    'read_ZAPO_SEGMT.json',
      'read_MARA.json',
      'read_zscm_plant_proty.json',
     'read_t179.json',
     'read_mseg.json',
    'read_vbrk.json',
    'read_vbrp.json',
     'read_mvke.json',
     'read_mbew.json',
     'read_likp.json',
     'read_MARA_VEN.json',
    'read_eina.json',
    'read_eine.json',
     'read_lips.json'
    #'read_TVM1.json'
]


for config_file_name in json_config_file_names:
    print("downloading "+config_file_name)
    get_ipython().run_line_magic('time', 'read_config_write_file(config_file_name,ECP)')

