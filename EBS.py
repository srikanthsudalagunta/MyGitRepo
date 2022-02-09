import cx_Oracle
from pandas import DataFrame, set_option, pandas as pd
import time
import logging
# import Mongodbutils
import os
from datetime import datetime
import codecs
import csv
OUTPUT_ENCODING = "utf-8"
BATCH_SIZE = 5000


def startEBSSession(Username, Password, Hostname, Port, ServiceName):
    try:
        os.environ['NLS_DATE_FORMAT'] = 'DD-MON-YYYY HH24:MI:SS'
        EBSSession = cx_Oracle.connect(
            "{}/{}@{}:{}/{}".format(Username, Password, Hostname, Port, ServiceName))
        logging.info('Oracle EBS Session Initiated')
        return EBSSession
    except:
        logging.exception('Oracle EBS Session Initiation Failure:')
        return None


def describeObject(EBSSession, ObjectName):
    try:
        cur = EBSSession.cursor()
        cur.execute("select column_name,column_id from all_tab_cols where hidden_column = 'NO' and owner = '{}' and table_name='{}'".format(
            ObjectName.split('.')[0], ObjectName.split('.')[1]))
        metadata = cur.fetchall()
        cur.close()
        fields_types = dict(metadata)
        logging.info(
            'Oracle EBS Object {} Metadata Extraction Completed'.format(ObjectName))
        logging.debug(fields_types)
        return fields_types
    except:
        cur.close()
        logging.exception(
            'Oracle EBS Object {} Metadata Extraction Failure'.format(ObjectName))
        return None


def QueryData(EBSSession, ExecuteStmnt, FileName):

    cols = []

    def append_row(filename, rows):
        with codecs.open(filename, "a", OUTPUT_ENCODING) as outfile:
            output = csv.writer(outfile, dialect='excel')
            for row in rows:
                output.writerow(row)

    def export_table_data(orcl, sql_query, filename):
        # output each table content to a separate CSV file
        # print(sql_query)
        # print(filename)
        with codecs.open(filename, "w", OUTPUT_ENCODING) as infile:
            pass

        sql = sql_query
        curs2 = orcl.cursor()
        curs2.prefetchrows = BATCH_SIZE
        curs2.arraysize = BATCH_SIZE
        curs2.execute(sql)
        # print(curs2.description)
        for col in curs2.description:
            cols.append(col[0])
        # print(cols)
        append_row(filename, [cols])

        row = 0
        done = False
        while not done:  # add table rows
            row_data = curs2.fetchmany()
            if len(row_data) < 1:
                done = True
            else:
                append_row(filename, row_data)
                row += len(row_data)
                print("{:,d}".format(row))
        curs2.close()
    try:
        logging.info('Query Execution Initiated: {} '.format(ExecuteStmnt))
        export_table_data(EBSSession, ExecuteStmnt, FileName)
        logging.info('Query Execution Completed.')
        logging.debug(cols)
        return cols
    except:
        logging.exception(
            'Query Execution Failed (query): {} '.format(ExecuteStmnt))
        return None


def extractData(ebs, ObjectsList, ObjectNames, LoadType, InitialExtractDate, Module, SubModuleDesc):
    try:
        # ebs = startEBSSession(Username,Password,Hostname,Port,ServiceName)
        logging.info(ebs)
        Result = {}
        logging.info(ObjectNames)
        objname = ''
        try:
            for Obj in ObjectNames.split(','):
                objname = Obj
                cur = ebs.cursor()
                cur.execute(
                    "select OWNER,OBJECT_NAME,OBJECT_TYPE from all_objects where object_name = '{}' and owner = '{}'".format(Obj, 'APPS'))
                print('1')
                objlist = cur.fetchall()
                print(objlist)
                if objlist[0][2] == 'SYNONYM':
                    cur.execute("select table_owner,table_name from all_synonyms where synonym_name = '{}' and owner = '{}'".format(
                        objlist[0][1], objlist[0][0]))
                    tabdetails = cur.fetchall()
                    tabname = tabdetails[0][1]
                    tabowner = tabdetails[0][0]
                    try:
                        cur.execute("select table_owner,table_name from all_synonyms where synonym_name = '{}' and owner = '{}'".format(
                            tabname, tabowner))
                        subtabdetails = cur.fetchall()
                        if len(subtabdetails) > 0:
                            tabname = subtabdetails[0][1]
                            tabowner = subtabdetails[0][0]
                    except:
                        tabname = tabdetails[0][1]
                        tabowner = tabdetails[0][0]
                elif objlist[0][2] in ('VIEW', 'TABLE'):
                    tabname = objlist[0][1]
                    tabowner = objlist[0][0]
                fields_types = describeObject(ebs, tabowner+'.'+tabname)
                cur.execute("SELECT (case when data_type = 'NUMBER' then (case when data_precision is not null then  column_name||' '||data_type||'('||data_precision||','||data_scale||')' else (case when table_name = 'GL_JE_LINES' and column_name in ('ENTERED_DR','ENTERED_CR','ACCOUNTED_DR','ACCOUNTED_CR') then column_name ||' NUMBER(38,7)' when table_name in ('OE_ORDER_HEADERS_ALL') and column_name in('MINISITE_ID','XML_MESSAGE_ID') then column_name ||' VARCHAR(4000)' when table_name in ('OE_ORDER_LINES_ALL') and column_name in('CUSTOMER_PAYMENT_TERM_ID','CUSTOMER_ITEM_NET_PRICE','COMMITMENT_ID','FULFILLED_QUANTITY2','UNIT_LIST_PRICE_PER_PQTY','UNIT_SELLING_PRICE_PER_PQTY','ORIGINAL_INVENTORY_ITEM_ID','ORIGINAL_ORDERED_ITEM_ID','LATE_DEMAND_PENALTY_FACTOR','MINISITE_ID') then column_name ||' VARCHAR(4000)' when table_name = 'OE_ORDER_LINES_ALL' and column_name in ('UNIT_SELLING_PRICE','UNIT_LIST_PRICE','UNIT_COST') then column_name ||' NUMBER(38,7)' when table_name in ('JTF_RS_RESOURCE_EXTNS') and column_name = 'ADDRESS_ID' then column_name ||' VARCHAR(4000)' when table_name = 'GL_BALANCES' and column_name in ('PERIOD_NET_DR','PERIOD_NET_CR','BEGIN_BALANCE_DR','BEGIN_BALANCE_CR') then column_name ||' NUMBER(38,7)' when table_name = 'MTL_MATERIAL_TRANSACTIONS' and column_name in ('ACTUAL_COST','TRANSACTION_COST','NEW_COST','PRIOR_COST','TRANSPORTATION_COST') then column_name ||' NUMBER(38,7)' when table_name = 'AP_INVOICES_ALL' and column_name in ('INVOICE_AMOUNT','AMOUNT_PAID','AMOUNT_APPLICABLE_TO_DISCOUNT','BASE_AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'AP_INVOICE_DISTRIBUTIONS_ALL' and column_name in ('AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'IBY_PAYMENTS_ALL' and column_name in ('PAYMENT_AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'AP_CHECKS_ALL' and column_name in ('AMOUNT') then column_name ||' NUMBER(38,7)' When table_name = 'AP_INVOICE_LINES_ALL' and column_name in ('AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'RA_CUSTOMER_TRX_LINES_ALL' and column_name in ('UNIT_STANDARD_PRICE','UNIT_SELLING_PRICE','QUANTITY_INVOICED','TAX_RATE') then column_name ||' NUMBER(38,7)' when table_name = 'AR_RECEIVABLE_APPLICATIONS_ALL' and column_name in ('ACCTD_AMOUNT_APPLIED_FROM','AMOUNT_APPLIED') then column_name ||' NUMBER(38,7)' When table_name = 'RA_CUSTOMER_TRX_ALL' and column_name in ('EXCHANGE_RATE') then column_name ||' NUMBER(38,7)' when table_name = 'AR_PAYMENT_SCHEDULES_ALL' and column_name in ('AMOUNT_APPLIED','EXCHANGE_RATE','AMOUNT_DUE_REMAINING','AMOUNT_DUE_ORIGINAL') then column_name ||' NUMBER(38,7)' when table_name = 'RA_CUST_TRX_LINE_GL_DIST_ALL' and column_name in ('AMOUNT','ACCTD_AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'AR_ADJUSTMENTS_ALL' and column_name in ('AMOUNT','ACCTD_AMOUNT') then column_name ||' NUMBER(38,7)' when table_name = 'AR_DISTRIBUTIONS_ALL' and column_name in ('AMOUNT_DR','AMOUNT_CR','ACCTD_AMOUNT_CR','ACCTD_AMOUNT_DR') then column_name ||' NUMBER(38,7)' when table_name = 'GL_DAILY_RATES' and column_name in ('CONVERSION_RATE') then column_name ||' NUMBER(38,7)' when table_name in ('AP_SUPPLIERS','AP_SUPPLIER_SITES_ALL','PO_VENDORS') and column_name = 'VALIDATION_NUMBER' then column_name ||' VARCHAR(4000)' when table_name in ('HZ_PARTIES') and column_name = 'DUNS_NUMBER' then column_name ||' VARCHAR(4000)' when table_name in ('MTL_SYSTEM_ITEMS_B') and column_name = 'COVERAGE_SCHEDULE_ID' then column_name ||' VARCHAR(4000)' when table_name in ('PO_LINES_ALL') and column_name = 'OKE_CONTRACT_HEADER_ID' then column_name ||' VARCHAR(4000)' when table_name in ('PO_DISTRIBUTIONS_ALL') and column_name = 'OKE_CONTRACT_LINE_ID' then column_name ||' VARCHAR(4000)' else column_name||' '||data_type end) end) WHEN data_type = 'VARCHAR2' THEN (case when data_length > 0 then column_name||' VARCHAR('||data_length||')' else column_name || ' VARCHAR(4000)' end) WHEN data_type = 'DATE' THEN column_name||' '||data_type ELSE column_name || ' STRING ' END) FROM all_tab_cols WHERE table_name = '{}' AND owner = '{}' AND hidden_column = 'NO' order by column_id".format(tabname, tabowner))
                res = cur.fetchall()
                col_string = ''
                tab_script = 'Create Table '+Obj+'('
                for i in res:
                    col_string = col_string+str(i[0])+','
                tab_script = tab_script+col_string.rstrip(',')+')'
                tab_def = tab_script
                cur.close()
                select_list = ''
                current_date_and_time_string = time.strftime("%Y%m%d_%H%M%S")
                FileName = "{Name}_{Time}.{Ext}".format(
                    Name=Obj, Time=current_date_and_time_string, Ext='CSV')
                AbsFileName = os.path.abspath(FileName)
                print(AbsFileName)
                if LoadType == 'F':
                    if InitialExtractDate is not None:
                        print('in if')
                        if 'CREATION_DATE' in ','.join(fields_types.keys()):
                            print('in nested if')
                            Data = QueryData(ebs, "Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME} Where Creation_Date >= '{ExtractDate}' ".format(
                                COL_NAME=','.join(fields_types.keys()), TAB_NAME=Obj, ExtractDate=InitialExtractDate), AbsFileName)
                        else:
                            # print('in else')
                            Data = QueryData(ebs, 'Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME}'.format(
                                COL_NAME=','.join(fields_types.keys()), TAB_NAME=Obj, ExtractDate=InitialExtractDate), AbsFileName)
                    else:
                        Data = QueryData(ebs, 'Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME}'.format(
                            COL_NAME=','.join(fields_types.keys()), TAB_NAME=Obj, ExtractDate=InitialExtractDate), AbsFileName)
                else:
                    # JobId = Mongodbutils.findJobId(ObjectsList)['JOB_ID']
                    # LastRefreshDate = Mongodbutils.findLastExtractDate(JobId)[
                        # 'REFRESH_DATE']
                    LastRefreshDate =  None
                    date_time_obj = datetime.strptime(
                        LastRefreshDate, '%Y-%m-%d %H:%M:%S.%f')
                    RefreshDate = date_time_obj.strftime('%d-%b-%Y')
                    if 'LAST_UPDATE_DATE' in ','.join(fields_types.keys()):
                        print('in if')
                        # print("Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME} where LAST_UPDATE_DATE >= '{LAST_REFRESH_DATE}'".format(COL_NAME=','.join(fields_types.keys()),TAB_NAME=Obj,LAST_REFRESH_DATE=RefreshDate))
                        Data = QueryData(ebs, "Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME} where NVL(LAST_UPDATE_DATE,CREATION_DATE) >= '{LAST_REFRESH_DATE}'".format(
                            COL_NAME=','.join(fields_types.keys()), TAB_NAME=Obj, LAST_REFRESH_DATE=RefreshDate), AbsFileName)
                    else:
                        # print('in else')
                        Data = QueryData(ebs, 'Select /*+ parallel(7) */ {COL_NAME} From {TAB_NAME}'.format(
                            COL_NAME=','.join(fields_types.keys()), TAB_NAME=Obj), AbsFileName)
                FileCols = Data
                print(FileCols)
                # for i in FileCols:
                #     idx = int(FileCols.index(i))+1
                #     select_list = select_list+'$'+str(idx)+' '+i+','
                # if Data:
                #     Result = {'ObjectName': Obj, 'FileName': AbsFileName, 'ColNames': FileCols, 'SelectFieldName': select_list,
                #               'FieldsTypes': fields_types, 'TabDefinition': tab_def, 'KeyColumns': Mongodbutils.findKeyCols('oracleebs', Module, Obj)['KeyColumns']}
                # else:
                #     continue
            # q.put(Result)
            # ebs.close()
            return Result
        except:
            # q.put(Result)
            logging.exception(
                f'Inner Execption for {objname} in extractData: ')
            return Result
    except:
        # q.put(Result)
        logging.exception('Outer Execption in extractData: ')
        return Result
