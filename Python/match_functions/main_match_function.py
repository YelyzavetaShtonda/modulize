import pandas as pd
import re
import openpyxl
import pymysql
import sys
sys.path.insert(0, 'D:\Робота\Python\data_recognition')
import data_recognition.func_to_identify_column_titles_new as func_to_identify_column_titles_new
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



#read excel file
name_table_1 = 'PSP_to_Airtable_1.xlsx'
name_table_2 = 'PSP_to_Airtable_1_copy.xlsx'
sheet_name_1 = 'Fibonatix'
sheet_name_2 = 'Fibonatix'
df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
width1 = len(df1.columns)
width2 = len(df2.columns)
length1 = len(df1.index)
length2 = len(df2.index)

column1_type = pd.DataFrame(func_to_identify_column_titles_new.main(name_table_1, sheet_name_1)[['Column Name', 'Final output', 'Score']])  #таблиця з назвами колонок і final категоризацією
column2_type = pd.DataFrame(func_to_identify_column_titles_new.main(name_table_2, sheet_name_2)[['Column Name', 'Final output', 'Score']])  #таблиця з назвами колонок і final категоризацією


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
    # print('her', list_with_emails_1 , list_with_emails_2)
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

        if re.search('date', column1_type.loc[i, 'Final output']) \
                or re.search('Date', column1_type.loc[i, 'Final output']) \
                or re.search('DATE', column1_type.loc[i, 'Final output']) \
                or re.search('time', column1_type.loc[i, 'Final output'])\
                or re.search('Time', column1_type.loc[i, 'Final output'])\
                or re.search('TIME', column1_type.loc[i, 'Final output']):

            list_with_datetime_1.append([column1_type.loc[i, 'Column Name'], column1_type.loc[i, 'Score']])
    # print(list_with_datetime_1)

    # find datetime in second table
    list_with_datetime_2 = []
    for i in range(len(column2_type.index)):

        if re.search('date', column2_type.loc[i, 'Final output']) \
                or re.search('Date', column2_type.loc[i, 'Final output']) \
                or re.search('DATE', column2_type.loc[i, 'Final output']) \
                or re.search('time', column2_type.loc[i, 'Final output'])\
                or re.search('Time', column2_type.loc[i, 'Final output'])\
                or re.search('TIME', column2_type.loc[i, 'Final output']):

            list_with_datetime_2.append([column2_type.loc[i, 'Column Name'], column2_type.loc[i, 'Score']])
    # print('here', list_with_datetime_1, list_with_datetime_2)
    return list_with_datetime_1, list_with_datetime_2

def write_add_trx_id_scores(i, matches_number_i, j, matches_number_j):
    # receives index of row and number of matches with this row from 1st table. receives the same from another table
    print(f'INCREASE SCORE BY {5*(matches_number_i-1)} and {5*(matches_number_j-1)} IN {i+2} ~ {j+2} rows')
    wb1 = openpyxl.load_workbook(filename=name_table_1, read_only=False)
    ws1 = wb1[sheet_name_1]
    ws1.cell(row=i + 2, column=width1 + 2).value = str(float(ws1.cell(row=i+2, column=width1 + 2).value[:-2]) + 5*(matches_number_i-1)) + ' %'


    wb2 = openpyxl.load_workbook(filename=name_table_2, read_only=False)
    ws2 = wb2[sheet_name_2]
    ws2.cell(row=j+2, column=width2 + 2).value = str(float(ws2.cell(row=j+2, column=width2 + 2).value[:-2]) + 5*(matches_number_j-1)) + ' %'

    wb1.save(name_table_1)
    wb2.save(name_table_2)

def additional_trx_id_scores(lists):
    # receives lists of columns and scores, which needed to be searched for additional scores
    print('i`m here')
    list_1, list_2 = lists
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
                print(f'change_scores (found {matches_number} matches per {j+2} row)')
                # add match number per row in dict
                if i in matches_in_every_row:
                    matches_in_every_row[i] += matches_number
                else:
                    matches_in_every_row[i] = matches_number
                if matches_number > 1:
                    write_add_trx_id_scores(i, matches_in_every_row[i], j, matches_number)





# function tries match by trx id, if no then by email, if no then by names, if no then by datetime
def final_scores():

    global df1, df2

    # list of dicts with matches and final scores per every match type (by trx_id, email, name, and datetime)
    return_list = []

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
        print(trx_id_lists)
        final_trx_score = final_connection_temp.query('`scoring API name` == "TRX ID temporary connection"')['score'][0]
        final_trx_id_matches = transaction_id_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_trx_score, columntype_score_1, columntype_score_2)
        # additional_trx_id_scores(trx_id_lists)
        print(final_trx_id_matches[0])
        print(final_trx_id_matches[1])
        return_list.append(['trx_id', final_trx_id_matches])

    if len(email_lists[0]) > 0 and len(email_lists[1]) > 0:
        df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
        df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
        column_name_1 = email_lists[0][0][0]
        column_name_2 = email_lists[1][0][0]
        columntype_score_1 = email_lists[0][0][1]
        columntype_score_2 = email_lists[1][0][1]
        print('by email')
        print(email_lists)
        final_email_score = final_connection_temp.query('`scoring API name` == "email temporary connection"')['score'][1]
        final_email_matches = email_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_email_score, columntype_score_1, columntype_score_2)
        # additional_trx_id_scores(email_lists)
        print(final_email_matches[0])
        print(final_email_matches[1])
        return_list.append(['email', final_email_matches])

    if len(name_lists[0]) > 0 and len(name_lists[1]) > 0:
        df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
        df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
        column_name_1 = name_lists[0][0][0]
        column_name_2 = name_lists[1][0][0]
        columntype_score_1 = name_lists[0][0][1]
        columntype_score_2 = name_lists[1][0][1]
        print('by name')
        print(name_lists)
        final_name_score = final_connection_temp.query('`scoring API name` == "name temporary connection"')['score'][2]
        final_name_matches = name_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_name_score, columntype_score_1, columntype_score_2)
        print(final_name_matches[0])
        print(final_name_matches[1])
        return_list.append(['name', final_name_matches])

    if len(datetime_lists[0]) > 0 and len(datetime_lists[1]) > 0:
        df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
        df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
        column_name_1 = datetime_lists[0][0][0]
        column_name_2 = datetime_lists[1][0][0]
        columntype_score_1 = datetime_lists[0][0][1]
        columntype_score_2 = datetime_lists[1][0][1]
        print('by datetime')
        print(datetime_lists)
        final_datetime_score = final_connection_temp.query('`scoring API name` == "date temporary connection"')['score'][3]
        final_datetime_matches = datetime_match.write_connections(name_table_1, df1, sheet_name_1, column_name_1, name_table_2, df2, sheet_name_2, column_name_2, final_datetime_score, columntype_score_1, columntype_score_2)
        print(final_datetime_matches[0])
        print(final_datetime_matches[1])
        return_list.append(['datetime', final_datetime_matches])

    # else:
    #     print('no match')
    #     final_datetime_score = final_connection_temp.query('`scoring API name` == "noname temporary connection"')['score'][4]

    return return_list

def write_final_score(dicts, final_coeff):
    """
    :param dicts: list of dictionaries with matches and final scores for both tables
    :param final_coeff: coefficient to be multiplied by existing score to receive final score
    :return: writes final scores to both tables
    """
    dict_1, dict_2 = dicts

    # import dataframes to find out the width
    df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
    df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
    width1 = len(df1.columns)
    width2 = len(df2.columns)

    # open 1 table
    wb1 = openpyxl.load_workbook(filename=name_table_1, read_only=False)
    ws1 = wb1[sheet_name_1]
    ws1.cell(row=1, column=width1 + 1).value = 'final_connection'
    ws1.cell(row=1, column=width1 + 2).value = 'final_score'
    # open 2 table
    wb2 = openpyxl.load_workbook(filename=name_table_2, read_only=False)
    ws2 = wb2[sheet_name_2]
    ws2.cell(row=1, column=width2 + 1).value = 'final_connection'
    ws2.cell(row=1, column=width2 + 2).value = 'final_score'


    print('dict1', dict_1)
    print('dict2', dict_2)

    #write matches ans final scores to 1st table
    for row, score in dict_1.items():
        if len(score) > 0:
            if len(score) == 1:
                if ws1.cell(row=row + 2, column=width1 + 1).value is None:
                    ws1.cell(row=row + 2, column=width1 + 1).value = f'{name_table_2}_{str(score[0][0] + 2)}'
                else:
                    ws1.cell(row=row + 2, column=width1 + 1).value = ws1.cell(row=row + 2, column=width1 + 1).value + '_' + str(score[0][0] + 2)
               # write score
                ws1.cell(row=row + 2, column=width1 + 2).value = str(score[0][1]*final_coeff/100) + ' %'

            # if for i row there are multiple matches with rows from 2 table
            else:
                for pair in score:
                    if ws1.cell(row=row + 2, column=width1 + 1).value is None:
                        ws1.cell(row=row + 2, column=width1 + 1).value = f'{name_table_2}_{str(pair[0] + 2)}'
                    else:
                        ws1.cell(row=row + 2, column=width1 + 1).value = ws1.cell(row=row + 2, column=width1 + 1).value + '_' + str(pair[0] + 2)

                    # write score
                    ws1.cell(row=row + 2, column=width1 + 2).value = str(pair[1] * final_coeff / 100) + ' %'

    # write matches ans final scores to 1st table
    for row, score in dict_2.items():
        if len(score) > 0:
            if len(score) == 1:
                if ws2.cell(row=row + 2, column=width2 + 1).value is None:
                    ws2.cell(row=row + 2, column=width2 + 1).value = f'{name_table_2}_{str(score[0][0] + 2)}'
                else:
                    ws2.cell(row=row + 2, column=width2 + 1).value = ws2.cell(row=row + 2, column=width2 + 1).value + '_' + str(score[0][0] + 2)
               # write score
                ws2.cell(row=row + 2, column=width2 + 2).value = str(score[0][1]*final_coeff/100) + ' %'

            # if for i row there are multiple matches with rows from 2 table
            else:
                for pair in score:
                    if ws2.cell(row=row + 2, column=width2 + 1).value is None:
                        ws2.cell(row=row + 2, column=width2 + 1).value = f'{name_table_2}_{str(pair[0] + 2)}'
                    else:
                        ws2.cell(row=row + 2, column=width2 + 1).value = ws2.cell(row=row + 2, column=width2 + 1).value + '_' + str(pair[0] + 2)

                    # write score
                    ws2.cell(row=row + 2, column=width2 + 2).value = str(pair[1] * final_coeff / 100) + ' %'

    # for row, score in dict_2.items():
    #     if len(score) > 0:
    #         if len(score) == 1:
    #             if ws2.cell(row=score[0][0] + 2, column=width2 + 1).value is None:
    #                 ws2.cell(row=score[0][0] + 2, column=width2 + 1).value = f'{name_table_1}_{str(row + 2)}'
    #             else:
    #                 ws2.cell(row=score[0][0] + 2, column=width2 + 1).value = ws2.cell(row=score[0][0] + 2, column=width2 + 1).value + '_' + str(row + 2)
    #             # write score
    #             ws1.cell(row=row + 2, column=width1 + 2).value = str(score[0][1]*final_coeff/100) + ' %'
    #
    #         # if for j row there are multiple matches with rows from 1 table
    #         else:
    #             for pair in score:
    #                 if ws2.cell(row=pair[0] + 2, column=width2 + 1).value is None:
    #                     ws2.cell(row=pair[0] + 2, column=width2 + 1).value = f'{name_table_1}_{str(row + 2)}'
    #                 else:
    #                     ws2.cell(row=pair[0] + 2, column=width2 + 1).value = ws2.cell(row=pair[0] + 2, column=width2 + 1).value + '_' + str(row + 2)
    #
    #                 # write score
    #                 ws2.cell(row=pair[0] + 2, column=width2 + 2).value = str(pair[1] * final_coeff / 100) + ' %'

    wb1.save(name_table_1)
    wb2.save(name_table_2)


def final_scores_2(list_of_dicts):
    """
    takes list of dicts with scores for each type and calculate final scores to pass it to write_final_score() func
    """


    # list of dicts with matches and scores for each type
    trx_id_dict = {}
    email_dict = {}
    name_dict = {}
    datetime_dict = {}

    # parse list with matches&scores of each type
    for type, dict in list_of_dicts:
        if type == 'trx_id':
            trx_id_dict = dict
        elif type == 'email':
            email_dict = dict
        elif type == 'name':
            name_dict = dict
        elif type == 'datetime':
            datetime_dict = dict

    if trx_id_dict:

        # if other dicts don`t exist
        if not (email_dict or name_dict or datetime_dict):
            write_final_score(trx_id_dict, 100)
        else:
            write_final_score(trx_id_dict, 75)


def main():
    list_of_dicts = final_scores()
    final_scores_2(list_of_dicts)

main()