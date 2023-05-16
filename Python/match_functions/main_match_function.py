import pandas as pd
import re
import openpyxl
import pymysql
import sys
sys.path.insert(0, 'D:\Робота\Python\data_recognition')
import func_to_identify_column_titles_new
import datetime_match
import transaction_id_match
import email_match
import name_match

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

# connect to sql cloud
conn = pymysql.connect(
    host="34.72.200.35",
    database="categorization_db",
    password='R6gK%L!J6034',
    user="root",
    port=3306,
    ssl_disabled=True
)

with conn.cursor() as cursor:
    # Read a single record
    sql = "SELECT * FROM Scoring"
    cursor.execute(sql, ())
    result = cursor.fetchone()
    scoring_df = pd.read_sql_query(sql, conn)


final_connection = scoring_df.query('`Function name` == "Connection"').reset_index(drop=True)
final_connection_temp = final_connection.query('`scoring type` == "temporary score"').reset_index(drop=True)
final_connection_final = final_connection.query('`scoring type` == "final score"').reset_index(drop=True)
# print(final_connection_temp)


#read excel file
name_table_1 = 'PSP_to_Airtable_1.xlsx'
name_table_2 = 'PSP_to_Airtable_1_copy.xlsx'
sheet_name_1 = 'AstroPay'
sheet_name_2 = 'AstroPay'
df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
width1 = len(df1.columns)
width2 = len(df2.columns)
length1 = len(df1.index)
length2 = len(df2.index)

column1_type = pd.DataFrame(func_to_identify_column_titles_new.main(name_table_1, sheet_name_1)[['Column Name', 'Final output', 'Score']])  #таблиця з назвами колонок і final категоризацією
column2_type = pd.DataFrame(func_to_identify_column_titles_new.main(name_table_2, sheet_name_2)[['Column Name', 'Final output', 'Score']])  #таблиця з назвами колонок і final категоризацією

# print(column2_type)

def find_trx_id():
    # find trx ids in first table
    list_with_ids_1 = []
    for i in range(len(column1_type.index)):

        if re.search('id', column1_type.loc[i, 'Final output']) or re.search('ID',
                               column1_type.loc[i, 'Final output']) or re.search('Id', column1_type.loc[i, 'Final output']):
            list_with_ids_1.append([column1_type.loc[i, 'Column Name'], column1_type.loc[i, 'Score']])
    # print(list_with_ids_1)

    # find trx ids in second table
    list_with_ids_2 = []
    for i in range(len(column2_type.index)):

        if re.search('id', column2_type.loc[i, 'Final output']) or re.search('ID',
                           column2_type.loc[i, 'Final output']) or re.search('Id', column2_type.loc[i, 'Final output']):
            list_with_ids_2.append([column2_type.loc[i, 'Column Name'], column2_type.loc[i, 'Score']])
    # print('here', list_with_ids_1, list_with_ids_2)
    return list_with_ids_1, list_with_ids_2

def find_email():
    # find emails in first table
    list_with_emails_1 = []
    for i in range(len(column1_type.index)):

        if re.search('email', column1_type.loc[i, 'Final output']) or re.search('Email', column1_type.loc[i, 'Final output']):
            list_with_emails_1.append([column1_type.loc[i, 'Column Name'], column1_type.loc[i, 'Score']])
    # print(list_with_emails_1)

    # find emails in second table
    list_with_emails_2 = []
    for i in range(len(column2_type.index)):

        if re.search('email', column2_type.loc[i, 'Final output']) or re.search('Email', column2_type.loc[i, 'Final output']) or re.search('Id', column2_type.loc[i, 'Final output']):
            list_with_emails_2.append([column2_type.loc[i, 'Column Name'], column2_type.loc[i, 'Score']])
    print('her', list_with_emails_1 , list_with_emails_2)
    return list_with_emails_1, list_with_emails_2

def find_names():
    # find names in first table
    list_with_names_1 = []
    # print(column1_type)
    for i in range(len(column1_type.index)):

        if re.search('Cardholder full name', column1_type.loc[i, 'Final output']) \
            or re.search('FIRST NAME', column1_type.loc[i, 'Final output']) \
            or re.search('LAST NAME', column1_type.loc[i, 'Final output']) \
            or re.search('Full name', column1_type.loc[i, 'Final output']):

            list_with_names_1.append([column1_type.loc[i, 'Column Name'], column1_type.loc[i, 'Score']])
    # print(list_with_names_1)

    # find names in second table
    list_with_names_2 = []
    for i in range(len(column2_type.index)):

        if  re.search('Cardholder full name', column2_type.loc[i, 'Final output']) \
            or re.search('FIRST NAME', column2_type.loc[i, 'Final output']) \
            or re.search('LAST NAME', column2_type.loc[i, 'Final output']) \
            or re.search('Full name', column2_type.loc[i, 'Final output']):

            list_with_names_2.append([column2_type.loc[i, 'Column Name'], column2_type.loc[i, 'Score']])
    # print(list_with_names_2)
    return list_with_names_1, list_with_names_2

def find_datetime():
    # find datetime in first table
    list_with_datetime_1 = []
    for i in range(len(column1_type.index)):

        if re.search('date', column1_type.loc[i, 'Final output']) or re.search('Date', column1_type.loc[i, 'Final output']) or re.search('DATE', column1_type.loc[i, 'Final output']):

            list_with_datetime_1.append([column1_type.loc[i, 'Column Name'], column1_type.loc[i, 'Score']])
    # print(list_with_datetime_1)

    # find datetime in second table
    list_with_datetime_2 = []
    for i in range(len(column2_type.index)):

        if re.search('date', column2_type.loc[i, 'Final output']) or re.search('Date', column2_type.loc[i, 'Final output']) or re.search('DATE', column2_type.loc[i, 'Final output']):

            list_with_datetime_2.append([column2_type.loc[i, 'Column Name'], column2_type.loc[i, 'Score']])
    # print('here', list_with_datetime_1, list_with_datetime_2)
    return list_with_datetime_1, list_with_datetime_2

def write_add_trx_id_scores(i, matches_number_i, j, matches_number_j):
    print(f'INCREASE SCORE BY {5*(matches_number_i-1)} and {5*(matches_number_j-1)} IN {i+2} ~ {j+2} rows')
    wb1 = openpyxl.load_workbook(filename=name_table_1, read_only=False)
    ws1 = wb1[sheet_name_1]
    ws1.cell(row=i + 2, column=width1 + 2).value = str(float(ws1.cell(row=i+2, column=width1 + 2).value[:-2]) + 5*(matches_number_i-1)) + ' %'


    wb2 = openpyxl.load_workbook(filename=name_table_2, read_only=False)
    ws2 = wb2[sheet_name_2]
    ws2.cell(row=j+2, column=width2 + 2).value = str(float(ws2.cell(row=j+2, column=width2 + 2).value[:-2]) + 5*(matches_number_j-1)) + ' %'

    wb1.save(name_table_1)
    wb2.save(name_table_2)

def additional_trx_id_scores(trx_id_lists):
    list_1, list_2 = trx_id_lists
    # number of matches in every i row form 1 table
    matches_in_every_row = {}
    # по кожній колонці 1 таблиці
    for col1, score1 in list_1:
        # по кожному рядку колонки з 1 таблиці
        for i in range(length1):
            # по кожному рядку 2 таблиці (через всі колонки)
            for j in range(length2):
                matches_number = 0
                # по кожній колонці з рядка в 2 таблиці
                for col2, score2 in list_2:
                    # have 2 columns from two tables. for every record from 1st column search matches with every row in 2nd column
                    # print(col1, '***', col2)
                    if df1[col1][i] == df2[col2][j]:
                        matches_number += 1
                # print(f'change_scores (found {matches_number} matches per {j+2} row)')
                # add match number per row in dict
                if i in matches_in_every_row:
                    matches_in_every_row[i] += matches_number
                else:
                    matches_in_every_row[i] = matches_number
                if matches_number > 1:
                    write_add_trx_id_scores(i, matches_in_every_row[i], j, matches_number)





# function tries match by trx id, if no then by email, if no then by names, if no then by datetime
def final_scores():
    # lists with column names which are recognized as specific type (trx_id, email, name or datetime)
    trx_id_lists = find_trx_id()
    email_lists = find_email()
    name_lists = find_names()
    datetime_lists = find_datetime()


    if len(trx_id_lists[0]) and len(trx_id_lists[1]):
        column_name_1 = trx_id_lists[0][0][0]
        column_name_2 = trx_id_lists[1][0][0]
        columntype_score_1 = trx_id_lists[0][0][1]
        columntype_score_2 = trx_id_lists[1][0][1]
        print('by trx')
        final_trx_score = final_connection_temp.query('`scoring API name` == "TRX ID temporary connection"')['score'][0]
        transaction_id_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_trx_score, columntype_score_1, columntype_score_2)
        additional_trx_id_scores(trx_id_lists)

    elif len(email_lists[0]) > 0 and len(email_lists[1]) > 0:
        column_name_1 = email_lists[0][0][0]
        column_name_2 = email_lists[1][0][0]
        columntype_score_1 = email_lists[0][0][1]
        columntype_score_2 = email_lists[1][0][1]
        print('by email')
        final_email_score = final_connection_temp.query('`scoring API name` == "email temporary connection"')['score'][1]
        email_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_email_score, columntype_score_1, columntype_score_2)
        # additional_trx_id_scores(trx_id_lists)

    elif len(name_lists[0]) > 0 and len(name_lists[1]) > 0:
        column_name_1 = name_lists[0][0][0]
        column_name_2 = name_lists[1][0][0]
        columntype_score_1 = name_lists[0][0][1]
        columntype_score_2 = name_lists[1][0][1]
        print('by name')
        final_name_score = final_connection_temp.query('`scoring API name` == "name temporary connection"')['score'][2]
        name_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_name_score, columntype_score_1, columntype_score_2)


    elif len(datetime_lists[0]) > 0 and len(datetime_lists[1]) > 0:
        column_name_1 = datetime_lists[0][0][0]
        column_name_2 = datetime_lists[1][0][0]
        columntype_score_1 = datetime_lists[0][0][1]
        columntype_score_2 = datetime_lists[1][0][1]
        print('by datetime')
        final_datetime_score = final_connection_temp.query('`scoring API name` == "date temporary connection"')['score'][3]
        datetime_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_datetime_score, columntype_score_1, columntype_score_2)
        # return datetime_lists

    else:
        print('no match')
        final_datetime_score = final_connection_temp.query('`scoring API name` == "noname temporary connection"')['score'][4]


final_scores()



