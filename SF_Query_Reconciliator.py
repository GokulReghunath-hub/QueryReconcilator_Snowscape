#importing packages

from logging import exception
import os
import pytz 
import pyodbc
import datetime 
import Config
import pandas as pd
import numpy as np
import snowflake.connector as sf
from random import randint

#creating first Snowflake connection
sf_conn_dict = {}
for i in range(1,int(Config.snowflake_db_count)+1):
    globals()[f"sf_conn{i}"] = sf.connect(
        user=Config.user,
        password=Config.password,
        account=Config.account,
        #authenticator='externalbrowser',
        #account='****',
        role = Config.role,
        database = f"Config.database{i}",
        schema = f"Config.schema{i}",
        warehouse = Config.warehouse,
    )
    globals()[f"cur{i}"] = globals()[f"sf_conn{i}"] .cursor()
    sf_conn_dict[f"Snowflake_Conn{i}"] = globals()[f"sf_conn{i}"]

#Fetching current session id
sessionId = randint(1000001,9999999)
print("Your Session ID is : ",sessionId)

#Function to check whether the column value is numeric or not
def is_num(n):
    if pd.isna(n):
        return False
    elif isinstance(n, (int, np.integer)) or isinstance(n, (float, np.float)):
        return True
    else:
        return False

#Declaring Input files for reading packages and testids
df_masterfile = pd.read_excel('InputFiles\Master_File_Accelerator.xlsx')
df_detailedconnectionfile = pd.read_excel('InputFiles\Detailed Connection File.xlsx')

#Taking User Inputs and defining TestIDs based on the user input
print("How you want to run?\n  1 = Package vise\n  2 = Test Case vise")
runchoice = int(input("Enter your choice : "))
if runchoice == 1:
    pckgeid = input("Enter the Package IDs seperated by comma : ")
    li_ippckgeidstr =pckgeid.split(",")
    li_ippckgeid = [float(i) for i in li_ippckgeidstr]
    df_packageselectedmaster = df_masterfile[df_masterfile['PACKAGE_ID'].isin(li_ippckgeid)]
    pckgtestid_li =[]
    for rows in df_packageselectedmaster.itertuples():
        testid_li =float(rows.TEST_ID)
        pckgtestid_li.append(testid_li)
    df_selectedmaster = df_masterfile[df_masterfile['TEST_ID'].isin(pckgtestid_li)]
    df_selecteddetailedconnectionfile = df_detailedconnectionfile[df_detailedconnectionfile['TEST_ID'].isin(pckgtestid_li)]
    update_testid_li = pckgtestid_li
else:
    iptestidt = input("Enter the Test IDs seperated by comma : ")
    li_iptestidstr =iptestidt.split(",")
    li_iptestid = [float(i) for i in li_iptestidstr]
    df_selectedmaster = df_masterfile[df_masterfile['TEST_ID'].isin(li_iptestid)]
    df_selecteddetailedconnectionfile = df_detailedconnectionfile[df_detailedconnectionfile['TEST_ID'].isin(li_iptestid)]
    update_testid_li = li_iptestid
print("Do you want to reconcile queries?\n  1 = YES\n  2 = NO")
reconcilechoice = int(input("Enter your choice : "))
if reconcilechoice == 1:
    acceptablevar = input("Enter acceptable percentage of difference in values : ")

    #Merging Master and Details input files based on user given TestIDs
    df_querysource = df_selectedmaster.merge(df_selecteddetailedconnectionfile, left_on='TEST_ID', right_on='TEST_ID', how='inner')
    df_querysource.sort_values("TEST_ID", axis = 0, ascending = True,
                    inplace = True, na_position ='last') 

    #Count Queries
    df_testquerycount = df_querysource[["TEST_ID","QUERY_FILENAME"]].groupby("TEST_ID").count().to_dict()
    dict_testquerycount = df_testquerycount["QUERY_FILENAME"]

    #Declaring Result dataframes           
    df_comparedoutput = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','ROW_ID', 'STATUS', 'DATA_GRANULARITY','DIMENSION_VALUES','MEASURE','MEASURE_VALUE','PERCENTAGE_DIFF','LATEST_RUN_FLAG','START_TIME','END_TIME'])
    df_summaryoutput = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','SQUAD','PACKAGE_NAME','TEST_NAME','STATUS', 'TOTAL_RECORD_COUNT','PASSED_RECORD_COUNT','FAILED_RECORD_COUNT','PASS_PERCENTAGE','LATEST_RUN_FLAG','START_TIME','END_TIME'])
    df_connectiondetails = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','TEST_NAME','CONNECTION', 'DATABASE','QUERY_PATH','SQL_DESCRIPTION','GRAIN_COL_COUNT','LATEST_RUN_FLAG'])
    df_failuresummary = pd.DataFrame(columns = ['SESSION_ID','PACKAGE_ID','TEST_ID','TOTAL_FAILED_RECORD_COUNT','FAILED_RECORD_COUNT', 'FAILURE_CATEGORY','LATEST_RUN_FLAG'])

    #Starts processing Queries from input files
    processingtestId = -1
    querycount = 0
    print("Processing SQL Queries...")
    for index, row in df_querysource.iterrows():
        
        #declaring result variables
        testid = row["TEST_ID"]
        dbenv = row["DATABASE"]
        packageid = row["PACKAGE_ID"]
        testname = row["TEST_NAME"]
        packagename = row["PACKAGE_NAME"]
        queryfilename = "Input SQL Queries\\" + row["QUERY_FILENAME"]
        sqldesciption = row["SQL_DESCRIPTION"]
        dbconnection = row["CONNECTION"]
        grain = int(row["GRAIN_COL_COUNT"])
        squad = row['SQUAD']

        #Reading current time
        now = datetime.datetime.now()
        start_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        #Reading Query file
        fd = open(queryfilename, 'r')
        sqlFile = fd.read()
        fd.close()

        #Writing connection details to dataframe
        df_connectiondetails = df_connectiondetails.append({'SESSION_ID':sessionId,'PACKAGE_ID' : packageid,'TEST_ID' : testid,'TEST_NAME' : testname,'CONNECTION' : dbconnection, 'DATABASE' : dbenv,'QUERY_PATH': queryfilename,'SQL_DESCRIPTION' : sqldesciption,'GRAIN_COL_COUNT':grain,'LATEST_RUN_FLAG':'Y'},ignore_index = True)
        
        if processingtestId == testid:
            querycount += 1
        else:
            querycount = 1

        #Executing querys in database
        try:
            globals()[f"df_queryresult_{querycount}"] = pd.read_sql_query(sqlFile,sf_conn_dict[row["CONNECTION"]])
            globals()[f"df_queryresult_{querycount}"].fillna("NULLVALUE",inplace = True)
        except exception as msg:
            print("Input Query Execution failed.: ", msg)

        if processingtestId == testid and querycount == dict_testquerycount[testid]:
            rowid = 1
            if grain != 0:
                queryresult_cols = globals()["df_queryresult_1"].columns
                graincollist = ','.join(globals()["df_queryresult_1"].columns[:grain])
                dimlist = list(globals()["df_queryresult_1"].columns[:grain])
                measurelist = list(globals()["df_queryresult_1"].columns[grain:])
                measurelistcols = [col + f"_tq1" for col in measurelist]
                globals()["df_queryresult_1"].columns = dimlist + measurelistcols

                for i in range(1,dict_testquerycount[testid]):
                    globals()[f"df_queryresult_{i+1}"].columns = queryresult_cols
                    measurelistcols = [col + f"_tq{i+1}" for col in measurelist]
                    globals()[f"df_queryresult_{i+1}"].columns = dimlist + measurelistcols
                    if i == 1:
                        df_merged = globals()[f"df_queryresult_{i}"]
                    df_merged = pd.merge(df_merged,globals()[f"df_queryresult_{i+1}"],how='outer',left_on=list(df_merged.columns[:grain]),right_on=list(globals()[f"df_queryresult_{i+1}"].columns[:grain]))

                for mergedindex, mergedrow in df_merged.iterrows():
                    strdimlist = [str(i) for i in mergedrow[:grain]]
                    dimlist = str(','.join(strdimlist))

                    for measurecol in queryresult_cols[grain:]:
                        mergeddimlist = []
                        status = 0
                        pervariance = 0
                        for i in range(1,dict_testquerycount[testid]):
                            querycol = measurecol+"_tq"+str(i)
                            nextquerycol = measurecol+"_tq"+str(i+1)          
                            if is_num(mergedrow[querycol]) and is_num(mergedrow[nextquerycol]):
                                pervariance = abs(round(((mergedrow[querycol] - mergedrow[nextquerycol])/mergedrow[querycol]) * 100,3))
                                if mergedrow[querycol] != mergedrow[nextquerycol] and pervariance > float(acceptablevar):
                                    status += 1
                            elif mergedrow[querycol] != mergedrow[nextquerycol]:
                                status += 1
                                pervariance = 100
                            if i == 1:
                                measurevaluelist = str(mergedrow[querycol])+" | "+str(mergedrow[nextquerycol])
                                perofdifflist = str(pervariance)
                            else:
                                measurevaluelist = measurevaluelist+" | "+str(mergedrow[nextquerycol])
                                perofdifflist = perofdifflist + ' | ' + str(pervariance)
                        
                            if pd.isna(mergedrow[querycol]):
                                mergeddimlist.append("NULLRECORD_TestQuery"+str(i))                  
                            else:
                                mergeddimlist.append(dimlist)
                            if i == dict_testquerycount[testid]-1:
                                if pd.isna(mergedrow[nextquerycol]):
                                    mergeddimlist.append("NULLRECORD_TestQuery"+str(i+1))
                                else:
                                    mergeddimlist.append(dimlist)
                        
                        if status == 0:
                            check_result = "Pass"
                        else:
                            check_result = "Fail"
                        resultdimlist = str(' | '.join(mergeddimlist))
                        #Fetching end time
                        now = datetime.datetime.now()
                        end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                        #Writing results to dataframe
                        df_comparedoutput = df_comparedoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'ROW_ID' : rowid,'STATUS' : check_result, 'DATA_GRANULARITY': str(graincollist), 'DIMENSION_VALUES' : resultdimlist ,'MEASURE' : measurecol,'MEASURE_VALUE' : measurevaluelist,'PERCENTAGE_DIFF' : perofdifflist,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time}, 
                        ignore_index = True)
                    rowid += 1
            else:
                #Processing records with zero grain
                status = 0
                pervariance = 0

                for i in range(1,dict_testquerycount[testid]):
                    if is_num(globals()[f"df_queryresult_{i}"].iloc[0,0]) and is_num(globals()[f"df_queryresult_{i+1}"].iloc[0,0]):
                        pervariance = abs(round(((globals()[f"df_queryresult_{i}"].iloc[0,0] - globals()[f"df_queryresult_{i+1}"].iloc[0,0])/globals()[f"df_queryresult_{i}"].iloc[0,0]) * 100,3))
                        if ~globals()[f"df_queryresult_{i}"].equals(globals()[f"df_queryresult_{i+1}"]) and pervariance > float(acceptablevar):
                            status += 1
                    elif mergedrow[querycol] != mergedrow[nextquerycol]:
                        status += 1
                        pervariance = 100

                            
                    if i == 1:
                        measurevaluelist = str(globals()[f"df_queryresult_{i}"].iloc[0,0])+" | "+str(globals()[f"df_queryresult_{i+1}"].iloc[0,0])
                        perofdifflist = str(pervariance)
                    else:
                        measurevaluelist = measurevaluelist+" | "+str(globals()[f"df_queryresult_{i+1}"].iloc[0,0])
                        perofdifflist = perofdifflist + " | " + str(pervariance)
                if status == 0:
                    check_result = "Pass"
                else:
                    check_result = "Fail"
                now = datetime.datetime.now()
                end_time = now.strftime("%Y-%m-%d %H:%M:%S")
                df_comparedoutput = df_comparedoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'ROW_ID' : rowid, 'STATUS' : check_result, 'DATA_GRANULARITY': 'NOT APPLICABLE', 'DIMENSION_VALUES' : df_queryresult_1.columns[0], 'MEASURE': 'NOT APPLICABLE' ,'MEASURE_VALUE' : measurevaluelist,'PERCENTAGE_DIFF' : perofdifflist,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time}, 
                    ignore_index = True)

            #Calculating Failure metrics
            rowcount = df_comparedoutput[df_comparedoutput["TEST_ID"] == testid].ROW_ID.nunique()
            fail_rowcount = df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid)].ROW_ID.nunique()
            pass_rowcount = int(rowcount) - int(fail_rowcount)
            if rowcount == 0:
                rowcount = 1
                print("!!! Error: SQL Query and Granularity Values doesn't match for Test_id :",testid)
            pass_percentage = round((pass_rowcount/rowcount) * 100,0)
            now = datetime.datetime.now()
            end_time = now.strftime("%Y-%m-%d %H:%M:%S")
            if df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid)].ROW_ID.nunique() == 0:
                status = "Pass"
            else:
                status = "Fail"
            
            #Writing Comparison summary
            df_summaryoutput = df_summaryoutput.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid,'SQUAD':squad,'PACKAGE_NAME':packagename,'TEST_NAME':testname, 'STATUS' : status, 'TOTAL_RECORD_COUNT': rowcount, 'PASSED_RECORD_COUNT' : pass_rowcount, 'FAILED_RECORD_COUNT': fail_rowcount ,'PASS_PERCENTAGE' : pass_percentage,'LATEST_RUN_FLAG':'Y','START_TIME' : start_time,'END_TIME' : end_time},ignore_index = True)
            
            #Calculating Failure row counts
            nullrecordcount = df_comparedoutput[(df_comparedoutput["STATUS"] == 'Fail') & (df_comparedoutput["TEST_ID"] == testid) & (df_comparedoutput["DIMENSION_VALUES"].str.contains("NULLRECORD_TestQuery"))].ROW_ID.nunique()
            failrecordcount_meas = fail_rowcount - nullrecordcount
            if nullrecordcount > 0:
                df_failuresummary = df_failuresummary.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'TOTAL_FAILED_RECORD_COUNT' : fail_rowcount, 'FAILED_RECORD_COUNT' : nullrecordcount, 'FAILURE_CATEGORY': "Dimension value combination is missing", 'LATEST_RUN_FLAG' : 'Y'},ignore_index = True)
            #Writing to Failure summary dataframe
            df_failuresummary = df_failuresummary.append({'SESSION_ID' : sessionId,'PACKAGE_ID': packageid,'TEST_ID' : testid, 'TOTAL_FAILED_RECORD_COUNT' : fail_rowcount, 'FAILED_RECORD_COUNT' : failrecordcount_meas, 'FAILURE_CATEGORY': "Measure mismatch between source and target", 'LATEST_RUN_FLAG' : 'Y'},ignore_index = True)

        else:
            processingtestId = testid
        

    df_failuresummary = df_failuresummary[df_failuresummary["FAILED_RECORD_COUNT"] != 0]

    #Writing results to Excel file
    now = datetime.datetime.now()
    print("Writing result to Excel file...")
    file_time = now.strftime("%Y%m%d%H%M")
    Filename = "OutputFiles\\"+"Query_Reconciliation_SessionID_"+str(sessionId)+"_DT_"+file_time+".xlsx"
    excelwriter = pd.ExcelWriter(Filename, engine = 'xlsxwriter')

    df_summaryoutput.to_excel (excelwriter, sheet_name='Result_Summary', index = False, header=True)
    df_failuresummary.to_excel (excelwriter, sheet_name='Faliure_Summary', index = False, header=True)
    df_comparedoutput.to_excel (excelwriter, sheet_name='Result_Detail', index = False, header=True)
    df_connectiondetails.to_excel (excelwriter, sheet_name='Test_Query_Detail', index = False, header=True)

    excelwriter.save()
    print("Results writen to ",Filename)

    #Printing results to console
    print("     *****************************************************        ")
    print(df_summaryoutput)
    print("     *****************************************************        ")

else:
    #Merging Master and Details input files based on user given TestIDs
    df_querysource = df_selectedmaster.merge(df_selecteddetailedconnectionfile, left_on='TEST_ID', right_on='TEST_ID', how='inner')
    df_querysource.sort_values("TEST_ID", axis = 0, ascending = True,
                    inplace = True, na_position ='last') 

    print("Processing SQL Queries...")
    processingtestId = -1
    querycount = 1
    now = datetime.datetime.now()
    file_time = now.strftime("%Y%m%d%H%M")
    Filename = "OutputFiles\\"+"Query_Reconciliation_SessionID_"+str(sessionId)+"_DT_"+file_time+".xlsx"
    excelwriter = pd.ExcelWriter(Filename, engine = 'xlsxwriter')
    for index, row in df_querysource.iterrows():
        #Reading Query file
        fd = open('Input SQL Queries\\' + row["QUERY_FILENAME"], 'r')
        sqlFile = fd.read()
        fd.close()
        try:
            df_queryresult= pd.read_sql_query(sqlFile,sf_conn_dict[row["CONNECTION"]])
            df_queryresult.fillna("NULLVALUE",inplace = True)
        except exception as msg:
            print("Input Query Execution failed.: ", msg)
            #Writing results to Excel file
        if processingtestId == row["TEST_ID"]:
            querycount += 1
        else:
            querycount = 1
        sheetname = "TestID_"+str(row["TEST_ID"]) + "_Query_"+str(querycount)
        df_queryresult.to_excel (excelwriter, sheet_name=sheetname, index = False, header=True)
        processingtestId = row["TEST_ID"]
    excelwriter.save()
    print("Results writen to ",Filename)
    
#Closing connections    
for i in range(1,int(Config.snowflake_db_count)+1):    
    globals()[f"sf_conn{i}"].close()
excelwriter.close()
