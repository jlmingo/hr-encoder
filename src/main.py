##########################################################################################################
#
# Codify and decodify HC files
#
# 
#
##########################################################################################################


from des import DesKey
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from os import getenv
from os import listdir
from os.path import isfile, join
from datetime import datetime
import xml.etree.ElementTree as ET
from datetime import datetime
import requests
from io import StringIO

xml_request = f'''<?xml version='1.0' encoding='UTF-8'?>
<call method='exportConfigurableModelData' callerName='LOI'>
    <credentials login='' password='' instanceCode=''/>
    <version name='' isDefault='false'/>
    <job jobNumber='0' pageNumber='1' pageSize='10'/>
    <modeled-sheet name='' isGlobal='true'/>
    <filters>
        <levels>
            <level name='EDPR' includeDescendants='true'/>
        </levels>
        <timeSpan start='' end=''/>
    </filters>
    <dimensions></dimensions>
    <rules includeZeroRows='false' includeRollups='false' markInvalidValues='false' markBlanks='false' timeRollups='YES'>
        <currency useCorporate='false' useLocal='false' override='EUR'/>
    </rules>
</call>'''

def Download_Adaptive(sheet):
    #parameters
    user=getenv("AdaptiveUser")
    password=getenv("AdaptivePassword")
    version=getenv("AdaptiveVersion")
    start_date=getenv("start_date")
    end_date=getenv("end_date")
    instanceCode = getenv("instanceCode")
    url = "https://api.adaptiveinsights.com/api/v19"
    
    #parameters
    root = ET.fromstring(xml_request)
    
    #set user and password
    root[0].set("login", user)
    root[0].set("password", password)
    root[0].set("instanceCode", instanceCode)

    #set version
    root[1].set("name", version)

    print(version)

    #set sheet
    root[3].set("name", sheet)    
    
    #set dates
    root[4][1].set("start", start_date)
    root[4][1].set("end", end_date)

    xmlstr = ET.tostring(root, encoding='unicode', method='xml')
    xmlstr = '<?xml version="1.0" encoding="utf8"?>' + xmlstr

    #print(xmlstr)
    
    res = requests.post(url, data=xmlstr, headers={'Content-Type': 'application/xml'})
    # print(res.text)
    root_response = ET.fromstring(res.text)
    dict_res = root_response.findall("output")[0].findall("data")[0].attrib
    jobNumber = dict_res["jobNumber"]

    try:
        numberOfPages = int(dict_res["numberOfPages"])
    except:
        numberOfPages = 1

    list_df = []
    s_text = root_response.findall("output")[0].findall("data")[0].text
    data = StringIO(s_text)
    df=pd.read_csv(data)
    list_df.append(df)
    
    for i in range(2, numberOfPages+1):
        #set jobNumber and pageNumber
        root[2].set("jobNumber", jobNumber)
        root[2].set("pageNumber", str(i)) 

        xmlstr = ET.tostring(root, encoding='unicode', method='xml')
        xmlstr = '<?xml version="1.0" encoding="utf8"?>' + xmlstr

        print(f"Launching request for page {i}")
        res = requests.post(url, data=xmlstr, headers={'Content-Type': 'application/xml'})
        root_response = ET.fromstring(res.text)
        try:
            s_text = root_response.findall("output")[0].findall("data")[0].text
        except Exception as e:
            print("Something failed:")
            print(e)
            print_msg = (ET.dump(root_response))
            raise ValueError
        data = StringIO(s_text)
        df=pd.read_csv(data)
        list_df.append(df)
    
    df_final = pd.concat(list_df, ignore_index=True)
    df_final.to_csv(f"{sheet}.csv", index=False)
    return df_final
    
def HR_getKey():
    Key = getenv("Password")
    print(Key)
    hexKey = bytes(bytearray.fromhex(Key))
    print(hexKey)
    ObKey = DesKey(hexKey)
    return ObKey

def HR_encrypt(DecodedInfo, ProvidedKey):
    #Encrypts the string with the information with the ProvidedKey
    if DecodedInfo:
        DecodedInfo = DecodedInfo.replace(u'\xa0', u' ')
    else:
        DecodedInfo = " "
    try:
        CodedInfo = ProvidedKey.encrypt(bytes(DecodedInfo, 'iso-8859-15'), padding=True)
        CodedInfohex = CodedInfo.hex()
    except:
        CodedInfo = ProvidedKey.encrypt(bytes(DecodedInfo, 'utf-16'), padding=True)
        CodedInfohex = CodedInfo.hex()
        CodedInfohex = "gg" + CodedInfohex
        print(CodedInfohex)
    
    return CodedInfohex

def HR_encrypt_JobTitle(DecodedJT, ProvidedKey):
    #Encrypts the string with the information with the ProvidedKey
    if DecodedJT:
        DecodedJT = DecodedJT.replace(u'\xa0', u' ')
    else:
        DecodedJT = " "
    
    if DecodedJT in ["wind power plant manager", "pv power plant manager", "shift operator", "dispatcher romania", "trade operator"] :
        CodedInfo = DecodedJT
    else :
        CodedInfo = HR_encrypt(DecodedJT, ProvidedKey)
    
    return CodedInfo

def HR_decrypt(CodedInfoHex, ProvidedKey):
    #Decrypts the string with the information with the ProvidedKey
    
    if CodedInfoHex[0:2] == 'gg':
        CodedInfoHex = CodedInfoHex[2:]
        codec = 'utf-16'
    else:
        codec = 'iso-8859-15'
        
    CodedInfo = bytes(bytearray.fromhex(CodedInfoHex))
    
    DecodedInfo = ProvidedKey.decrypt(CodedInfo, padding=True)
    
    DecodedInfo = DecodedInfo.decode(codec)
    
    return DecodedInfo

def HR_decrypt_JobTitle(EncodedJT, ProvidedKey):
    #Decrypts the string with the information with the ProvidedKey
    if EncodedJT in ["wind power plant manager", "pv power plant manager", "shift operator", "dispatcher romania", "trade operator"] :
        DecodedInfo = EncodedJT
    else :
        DecodedInfo = HR_decrypt(EncodedJT, ProvidedKey)
    
    return DecodedInfo

def generate_previous_years_columns(column, current_year):
    '''Given a column, it returns a list for previous years
    '''
    list_col = []
    for i in range(current_year-1, -1, -1):
        print(i)
        if i != 0:
            append_col = column.replace(f"N+{current_year}", f"N+{i}")
            list_col.append(append_col)
        else:
            append_col = column.replace(f" N+{current_year}", "")
            list_col.append(append_col)
    return list_col

if __name__ == "__main__":
    load_dotenv(".env")
    ObKey = HR_getKey()
    

    str_message = '''
    Please select program mode:
    1 to Encode
    2 to Get Report
    3 Just Decode
    4 to Get Report and Totals
    0 to Exit: 
    '''
    program_mode = int(input(str_message))
    
    while program_mode != 0:
        dtypes_headcount = {
            'Situacion': "str",
            'Company': "str",
            'Global ID': "str",
            'Employee Name': "str",
            'Hay Level': "str",
            'CBA': "str",
            'Country of Work': "str",
            'City of work': "str",
            'Area': "str",
            'Platform': "str",
            'Dirección': "str",
            'Departamento': "str",
            'Country of Contract': "str",
            'Job Title': "str",
            'Gender': "str",
            'Reducción de Jornada': "str",
            'Expat': "str",
            'Elig Pension Plan': "str",
            'CECO': "str",
            'Department SIM': "str",
            'End Date': "str",
            'Level': "str",
            'Country': "str",
            'CostCentre_EU': "str",
            'CostCentre_CH': "str",
            'Employee ID': "str",
            'Bonus upside': "float64",
            'Salario Anual': "float64",
            'Duty Call carga': "float64",
            'PensionPlan Carga': "float64",
            'Seguro de vida carga': "float64",
            'CompanyCar carga': "float64",
            'Varios PT Carga': "float64"
            }

        if program_mode in [0,1]:
            print("----------------------------------")
            print("- Encode Load File")           
            print("----------------------------------")

            #get list of files
            source_path = getenv("Source_Folder")
            source_files = [join(source_path, f) for f in listdir(source_path) if isfile(join(source_path, f)) & ~f.startswith("~")]
            print(source_files)
            
            #get list of dataframes
            try:
                df_headcounts = [pd.read_excel(file_name, sheet_name="Dataload", dtype=dtypes_headcount) for file_name in source_files]
            except PermissionError:
                print("Error reading the Excel file, please close all Excel files and try again.")
            
            #create and transform headcount dataframe
            df_headcount = pd.concat(df_headcounts)
            df_headcount["Global ID"] = df_headcount["Global ID"].map(lambda x: HR_encrypt(x, ObKey))
            df_headcount["Employee Name"] = df_headcount["Employee Name"].map(lambda x: HR_encrypt(x, ObKey))
            df_headcount["Job Title"] = df_headcount["Job Title"].map(lambda x: HR_encrypt_JobTitle(x, ObKey))
            
            load_folder = getenv("Load_Folder")
            output_path = join(load_folder, "AdaptiveLoad" + datetime.now().strftime("%Y%m%d%H%M%S") + ".xlsx")
            df_headcount.to_excel(output_path, sheet_name='Dataload', index=False)

        elif program_mode in [3]:
            print("----------------------------------")
            print("- Just Decode")           
            print("----------------------------------")
            Reports_Decoded_Folder = getenv("Reports_Decoded_Folder")
            
            df_report = pd.read_excel(join(Reports_Decoded_Folder, "Report_encoded.xlsx"), sheet_name='Report', dtype=dtypes_headcount)
            
            df_report["Employee ID"] = df_report["Employee ID"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Employee Name"] = df_report["Employee Name"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Job Title"] = df_report["Job Title"].map(lambda x: HR_decrypt_JobTitle(x, ObKey))
            
            df_report.to_excel(join(Reports_Decoded_Folder, "Report_decoded"+datetime.now().strftime("%Y%m%d%H%M%S")+".xlsx"), sheet_name='Report', index=False)

        elif program_mode in [4]:
            print("----------------------------------")
            print("- to Get Report and Totals")           
            print("----------------------------------")
            source_path = getenv("Reports_Folder")
            
            #europe
            df_report_EU = Download_Adaptive("Human Resources EU+CH+OF")

            (df_report_EU['Periculosidade_BR'],
            df_report_EU['SeguroMed_seguromedico'],
            df_report_EU['SeguroMed_examenes_medicos'],
            df_report_EU['SegMedicoOdontologicoBR'],
            df_report_EU['Seguro_Med_AuxilioBR'],
            df_report_EU['Canteen_BR'],
            df_report_EU['Tarjeta_Transporte_BR'],
            df_report_EU['Gratificacion_de_ferias'],
            df_report_EU['Seguridad_Social_FGTS'],
            df_report_EU['Seguridad_Social_Total'],
            df_report_EU['Horas_extra2']) = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            
            columns_float = [
                "Salary",
                "Bonus Amount",
                "SeguroMedico_Sumatorio de conceptos",
                "Duty Call Final",
                "Seguro Vida Final",
                "CompanyCar Final",
                "Varios Final",
                "Seguridad Social",
                "PensionPlan Final",
                "Parking_all"
            ]

            dict_cols = {}
            number_of_years = 5
            for i in range(5):
                if i == 0:
                    dict_cols[i] = columns_float
                else:
                    dict_cols[i] = [col + f" N+{i}" for col in columns_float if col != "Parking_all"]
            
            #convert floats
            for i, list_of_columns in dict_cols.items():
                for col in list_of_columns:
                    df_report_EU.loc[:, col] = df_report_EU[col].astype("str").str.replace(',', '').astype("float64").fillna(0)
                    
                    if i == 0:
                        pass
                    elif i == 1:
                        col_py = col.replace(f" N+{str(i)}", "")
                        df_report_EU.loc[:, col] = df_report_EU[col].fillna(0) - df_report_EU[col_py].fillna(0)
                    else:
                        #convert cumulative to single year figures
                        col_list_py = generate_previous_years_columns(col, i)
                        df_report_EU.loc[:, col] = df_report_EU[col].fillna(0) - df_report_EU[col_list_py].fillna(0).sum(axis=1)
            
            # without bonus amount
            columns_without_bonus = [
                "Salary", "SeguroMedico_Sumatorio de conceptos", "Duty Call Final",
                "Seguro Vida Final", "CompanyCar Final", "Varios Final",
                "Seguridad Social", "PensionPlan Final", "Parking_all"
            ]

            df_report_EU.loc[:, 'Total without Bonus'] = df_report_EU[columns_without_bonus].sum(axis=1)
            for i in range(1, 5):
                col_list_cy = [col + f" N+{i}" for col in columns_without_bonus if col != "Parking_all"]
                df_report_EU.loc[:, f'Total without Bonus N+{i}'] =  df_report_EU[col_list_cy].sum(axis=1)
                
            #with bonus amount
            df_report_EU.loc[:, 'Total with Bonus'] =  df_report_EU.loc[:, 'Total without Bonus'] + df_report_EU['Bonus Amount'].fillna(0) 
            for i in range(1, 5):
                df_report_EU.loc[:, f'Total with Bonus N+{i}'] =  df_report_EU.loc[:, f'Total without Bonus N+{i}'] + df_report_EU[f'Bonus Amount N+{i}'].fillna(0) 


            #add columns
            df_report_EU["Seguridad_Social_Total"] = df_report_EU["Seguridad Social"]

            #Brazil 
                  
            df_report_BR = Download_Adaptive("Human Resources Brazil")
            
            (df_report_BR['PensionPlan Carga'],
            df_report_BR['PensionPlan Extra'],
            df_report_BR['PensionPlan Final'],
            df_report_BR['Seguro de vida carga'],
            df_report_BR['Seguro Vida Extra'],
            df_report_BR['Seguro Vida Final'], 
            df_report_BR['CompanyCar carga'],
            df_report_BR['CompanyCar Extra'],
            df_report_BR['CompanyCar Final'],
            df_report_BR['Parking_all'], 
            df_report_BR['Varios PT Carga'],
            df_report_BR['Varios PT Extra'],
            df_report_BR['FTE Pension Plan'],
            df_report_BR['CostCentre_CH'],
            df_report_BR['Duty Call carga'],
            df_report_BR['Duty Call Extra'],
            df_report_BR['Duty Call Final']) = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            
            columns_float = [
                "Salary",
                "Bonus Amount",
                "SeguroMedico_Sumatorio de conceptos",
                "Duty Call",
                "Seguro_Vida",
                "Seguridad_Social_Total",
                "Periculosidade_BR",
                "Canteen_BR",
                "Tarjeta_Transporte_BR",
                "Gratificacion_de_ferias",
                "Pension_Plan",
                "Varios Final"
            ]

            dict_cols = {}
            number_of_years = 5
            for i in range(5):
                if i == 0:
                    dict_cols[i] = columns_float
                else:
                    dict_cols[i] = [col + f" N+{i}" for col in columns_float]
            
            #convert floats
            for i, list_of_columns in dict_cols.items():
                for col in list_of_columns:
                    df_report_BR.loc[:, col] = df_report_BR[col].astype("str").str.replace(',', '').astype("float64").fillna(0)
                    if i == 0:
                        pass
                    elif i == 1:
                        col_py = col.replace(f" N+{i}", f"")
                        df_report_BR.loc[:, col] = df_report_BR[col].fillna(0) - df_report_BR[col_py].fillna(0)
                    else:
                        #convert cumulative to single year figures
                        col_list_py = generate_previous_years_columns(col, i)
                        df_report_BR.loc[:, col] = df_report_BR[col].fillna(0) - df_report_BR[col_list_py].fillna(0).sum(axis=1)
            
            # without bonus amount
            columns_without_bonus = [
                "Salary", "SeguroMedico_Sumatorio de conceptos", "Duty Call",
                "Seguro_Vida", "CompanyCar", 'Seguridad_Social_Total', "Periculosidade_BR",
                "Canteen_BR", "Tarjeta_Transporte_BR", "Gratificacion_de_ferias",
                "Pension_Plan", "Varios Final"
            ]

            df_report_BR.loc[:, 'Total without Bonus'] = df_report_BR[columns_without_bonus].sum(axis=1)
            for i in range(1, 5):
                col_list_cy = [col + f" N+{i}" for col in columns_without_bonus if col != "Parking_all"]
                df_report_BR.loc[:, f'Total without Bonus N+{i}'] =  df_report_BR[col_list_cy].sum(axis=1)
                
            #with bonus amount
            df_report_BR.loc[:, 'Total with Bonus'] =  df_report_BR.loc[:, 'Total without Bonus'] + df_report_BR['Bonus Amount'].fillna(0) 
            for i in range(1, 5):
                df_report_BR.loc[:, f'Total with Bonus N+{i}'] =  df_report_BR.loc[:, f'Total without Bonus N+{i}'] + df_report_BR[f'Bonus Amount N+{i}'].fillna(0) 

            #add columns for other totals
            df_report_BR["Duty Call Final"] = df_report_BR["Duty Call"]
            df_report_BR["PensionPlan Final"] = df_report_BR["Pension_Plan"]
            df_report_BR["Seguro Vida Final"] = df_report_BR["Seguro_Vida"]
            df_report_BR["CompanyCar Final"] = df_report_BR["CompanyCar"]

            
            df_report_NA = Download_Adaptive("Human Resources NA")
            
            df_report_NA.rename(columns={'NH Situation' : 'Situacion','Direction' : 'Dirección','Department' : 'Departamento'}, inplace=True)
        
            df_report = pd.concat([df_report_EU, df_report_BR, df_report_NA])

            #relocate social security column near to "Seguridad Social"
            index_ = df_report.columns.get_loc("Seguridad Social")
            df_report.insert(index_+1, column="Seguridad_Social_Total", value=df_report.pop("Seguridad_Social_Total"))

            Reports_Decoded_Folder = getenv("Reports_Decoded_Folder")

            df_report.to_excel(join(Reports_Decoded_Folder, "Report_encoded.xlsx"), sheet_name='Report', index=False)
            
            df_report["Employee ID"] = df_report["Employee ID"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Employee Name"] = df_report["Employee Name"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Job Title"] = df_report["Job Title"].map(lambda x: HR_decrypt_JobTitle(x, ObKey))
            
            df_report.to_excel(join(Reports_Decoded_Folder, "Report_decoded"+datetime.now().strftime("%Y%m%d%H%M%S")+".xlsx"), sheet_name='Report', index=False)
    
        else :
            print("----------------------------------")
            print("- to Get only Report - avoid fail in totals")           
            print("----------------------------------")
            sourcePath = getenv("Reports_Folder")
            
            df_report_EU = Download_Adaptive("Human Resources EU+CH+OF")

            df_report_EU['Periculosidade_BR'], df_report_EU['SeguroMed_seguromedico'], df_report_EU['SeguroMed_examenes_medicos'], df_report_EU['SegMedicoOdontologicoBR'], df_report_EU['Seguro_Med_AuxilioBR'], df_report_EU['Canteen_BR'], df_report_EU['Tarjeta_Transporte_BR'], df_report_EU['Gratificacion_de_ferias'], df_report_EU['Seguridad_Social_FGTS'], df_report_EU['Seguridad_Social_Total'], df_report_EU['Horas_extra2'] = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            
            df_report_EU.dtypes
                  
            df_report_BR = Download_Adaptive("Human Resources Brazil")
            
            df_report_BR['PensionPlan Carga'], df_report_BR['PensionPlan Extra'], df_report_BR['PensionPlan Final'], df_report_BR['Seguro de vida carga'], df_report_BR['Seguro Vida Extra'], df_report_BR['Seguro Vida Final'], df_report_BR['CompanyCar carga'], df_report_BR['CompanyCar Extra'], df_report_BR['CompanyCar Final'], df_report_BR['Parking_all'], df_report_BR['Varios PT Carga'], df_report_BR['Varios PT Extra'], df_report_BR['Varios Final'], df_report_BR['FTE Pension Plan'], df_report_BR['CostCentre_CH'], df_report_BR['Duty Call carga'], df_report_BR['Duty Call Extra'], df_report_BR['Duty Call Final'] = [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]
            
            df_report_BR.dtypes
            
            df_report_NA = Download_Adaptive("Human Resources NA")
            
            df_report_NA.rename(columns={'NH Situation' : 'Situacion','Direction' : 'Dirección','Department' : 'Departamento'}, inplace=True)
        
            df_report = pd.concat([df_report_EU, df_report_BR, df_report_NA])

            Reports_Decoded_Folder = getenv("Reports_Decoded_Folder")
            
            df_report.to_excel(join(Reports_Decoded_Folder, "Report_encoded.xlsx"), sheet_name='Report', index=False)
            
            df_report["Employee ID"] = df_report["Employee ID"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Employee Name"] = df_report["Employee Name"].map(lambda x: HR_decrypt(x, ObKey))
            df_report["Job Title"] = df_report["Job Title"].map(lambda x: HR_decrypt_JobTitle(x, ObKey))
            
            df_report.to_excel(join(Reports_Decoded_Folder, "Report_decoded"+datetime.now().strftime("%Y%m%d%H%M%S")+".xlsx"), sheet_name='Report', index=False)
              
        program_mode = int(input("Please select program mode (1 to Encode, 2 to Get Report, 3 Just Decode, 4 to Get Report and Totals, 0 to Exit): ")) #1: Encode, 2: Get Report, 0: Exit
