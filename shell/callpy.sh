#!/usr/bin/python
#*****************************************************************************************#
# Nombre      : callpy.sh                                                                 #
#                                                                                         #
# Descripcion : Hace la llamada al programe en Python                                     #
#                                                                                         #
# Autores     : Christian Calderon / Cristian Quintana / Felipe Cruz                      #
# Empresa     : TCS                                                                       #
# Sintaxis    : sh callmeback.sh                                                          #
# Fecha       : 16/01/2021                                                                #
#*****************************************************************************************#

import pandas as pd
from datetime import datetime
import numpy as np
import os
input_dir="inputExcels/"
for dir,d,filename in os.walk(input_dir):
    for file in filename :
        items_in_dir=file
        xl = pd.ExcelFile(dir+file)
        SISTEMA=file.split(".")[0].split("_")[1].upper()
        D_YYYY=file.split(".")[0].split("_")[0][0:4]
        D_MM=file.split(".")[0].split("_")[0][4:6]
        sheetnames=xl.sheet_names  # see all sheet names
        sheetneed=[x for x in sheetnames if x.startswith('Activos')]
        generation_date_time=str(datetime.now())
        P_YYYY=generation_date_time.split(" ")[0].split("-")[0]
        P_MMs=generation_date_time.split(" ")[0].split("-")[1]
        P_DD=generation_date_time.split(" ")[0].split("-")[2]
        P_HH=generation_date_time.split(" ")[1].split(".")[0].split(":")[0]
        P_MM=generation_date_time.split(" ")[1].split(".")[0].split(":")[1]
        for sheetname in sheetneed:
            if sheetname == 'Activos Bancos 1':
                df=xl.parse('Activos Bancos 1')
                df1=df.set_index(df.columns[0],drop=True)
                df1.dropna(axis=0,how="all",inplace=True)
                df1.dropna(axis=1,how="all",inplace=True)
                df1.reset_index(drop=False,inplace=True)
                df1.columns=df1.iloc[0].tolist()
                df1.drop(df1.index[0],axis=0,inplace=True)
                df1.drop(df1.index[-1],axis=0,inplace=True)
                Header="Header"+"||"+SISTEMA+"||"+sheetname+"||"+D_YYYY+\
                D_MM+"||"+P_YYYY+P_MMs+"||"+P_HH+P_MM+"||"+str(len(df1))
                np.savetxt('outputInterfaces/'+"GDD_M_"+SISTEMA+'_Activos1_'\
                           +D_YYYY+D_MM+'.dat', df1.values, delimiter='||', fmt='%s',\
                           encoding='utf-8',header=Header,comments="")
            if sheetname == 'Activos Bancos 2':
                df=xl.parse('Activos Bancos 2')
                df1=df.set_index(df.columns[0],drop=True)
                df1.dropna(axis=0,how="all",inplace=True)
                df1.dropna(axis=1,how="all",inplace=True)
                df1.reset_index(drop=False,inplace=True)
                df1.columns=df1.iloc[0].tolist()
                df1.drop(df1.index[0:5],axis=0,inplace=True)
                df1.drop(df1.index[-1],axis=0,inplace=True)
                Header="Header"+"||"+SISTEMA+"||"+sheetname+"||"+D_YYYY+\
                D_MM+"||"+P_YYYY+P_MMs+"||"+P_HH+P_MM+"||"+str(len(df1))
                np.savetxt('outputInterfaces/'+"GDD_M_"+SISTEMA+'_Activos2_'\
                           +D_YYYY+D_MM+'.dat', df1.values, delimiter='||', fmt='%s',\
                           encoding='utf-8',header=Header,comments="")
