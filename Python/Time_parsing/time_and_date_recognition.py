# import pandas as pd
import re
# import openpyxl
# import pymysql

# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)

# # connect to sql cloud
# conn = pymysql.connect(
#     host="34.72.200.35",
#     database="categorization_db",
#     password='R6gK%L!J6034',
#     user="root",
#     port=3306,
#     ssl_disabled=True
# )
#
# with conn.cursor() as cursor:
#     # Read a single record
#     sql = "SELECT * FROM Scoring"
#     cursor.execute(sql, ())
#     result = cursor.fetchone()
#     scoring_df = pd.read_sql_query(sql, conn)
#     # print(scoring_df)


#read excel file
# name_table_1 = 'PSP_to_Airtable_1.xlsx'
# name_table_2 = 'PSP_to_Airtable_1_copy.xlsx'
# sheet_name_1 = 'Volt'
# sheet_name_2 = 'Volt'
# df1 = pd.read_excel(name_table_1, sheet_name=sheet_name_1)
# df2 = pd.read_excel(name_table_2, sheet_name=sheet_name_2)
# df1_column = 'created_at'
# df2_column = 'created_at'
# width1 = len(df1.columns)
# width2 = len(df2.columns)
# length1 = len(df1.index)
# length2 = len(df2.index)

# read scoring file from excel
# scoring_df = pd.read_excel('scoring.xlsx')


# makes table for datetime scoring
# timestamp_scoring = scoring_df.query('`Function name` == "datetimeconnection"').reset_index(drop=True)
# print(timestamp_scoring)


def parse_date(data):
    data = str(data).strip()
    if match_ := re.match('(\d{4}).{1}(\d{1,2}).{1}(\d{1,2})\D+(\d{1,2}).{1}(\d{1,2}).{1}(\d{1,2}).{1}(\d{1,3})(\+\d{2})', str(data)):# yyyy mm ddThh mm ss mm +UTC...   like 2023-03-30T15:51:15:555+0200
        return [match_.group(3), match_.group(2), match_.group(1), match_.group(4), match_.group(5),
                    match_.group(6), match_.group(7), match_.group(8)]
    elif match_ := re.match('(\d{4}).{1}(\d{1,2}).{1}(\d{1,2})\D+(\d{1,2}).{1}(\d{1,2}).{1}(\d{1,2})(\+\d{2})', str(data)): # yyyy mm ddThh mm ss +UTC...   like 2023-03-30T15:51:15+0200
        return [match_.group(3), match_.group(2), match_.group(1), match_.group(4), match_.group(5),
                match_.group(6), None, match_.group(7)]
    elif match_ := re.match('(\d{4}).{1}(\d{2}).{1}(\d{2})\s+(\d{1,2}).{1}(\d{2}).{1}(\d{2}).{1}(\d{2})', str(data)):   # yyyy mm dd hh mm ss mm
        return [match_.group(3), match_.group(2), match_.group(1),match_.group(4),match_.group(5),match_.group(6), match_.group(7)]
    elif match_ := re.match('(\d{2}).{1}(\d{2}).{1}(\d{4})\s+(\d{1,2}).{1}(\d{2}).{1}(\d{2}).{1}(\d{2})', str(data)):   # dd mm yyyy hh mm ss mm
        return [match_.group(1), match_.group(2), match_.group(3),match_.group(4),match_.group(5),match_.group(6), match_.group(7)]
    elif match_ := re.match('(\d{4}).{1}(\d{2}).{1}(\d{2})\s+(\d{1,2}).{1}(\d{2}).{1}(\d{2})', str(data)):              # yyyy mm dd hh mm ss
        return [match_.group(3), match_.group(2), match_.group(1),match_.group(4),match_.group(5),match_.group(6), None]
    elif match_ := re.match('(\d{2}).{1}(\d{2}).{1}(\d{4})\s+(\d{1,2}).{1}(\d{2}).{1}(\d{2})', str(data)):              # dd mm yyyy hh mm ss
        return [match_.group(1), match_.group(2), match_.group(3),match_.group(4),match_.group(5),match_.group(6), None]
    elif match_ := re.match('(\d{4}).{1}(\d{2}).{1}(\d{2})\s+(\d{1,2}).{1}(\d{2})', str(data)):                         # yyyy mm dd hh mm  2023-04-22 10:05..
        return [match_.group(3), match_.group(2), match_.group(1),match_.group(4),match_.group(5), None, None]
    elif match_ := re.match('(\d{2}).{1}(\d{2}).{1}(\d{4})\s+(\d{1,2}).{1}(\d{2}).{1}(\d{2})', str(data)):              # dd mm yyyy hh mm
        return [match_.group(1), match_.group(2), match_.group(3),match_.group(4),match_.group(5),None, None]


    elif match_ := re.match('(\d{4}).{1}(\d{2}).{1}(\d{2})', str(data)):                                                # yyyy mm dd
        return [match_.group(3), match_.group(2), match_.group(1), None, None, None, None]
    elif match_ := re.match('(\d{2}).{1}(\d{2}).{1}(\d{4})', str(data)):                                                # dd mm yyyy
        return [match_.group(1), match_.group(2), match_.group(3), None, None, None, None]
    elif match_ := re.match('(\d{4})', str(data)):                                                                      # yyyy
        return [None, None, match_.group(1), None, None, None, None]


    elif match_ := re.match('(\d{2}).{1}(\d{2})', str(data)):                                                           # dd mm or mm dd
        if int(match_.group(2)) > 12:
            return [match_.group(2), match_.group(1), None, None, None, None, None]                                     # mm dd
        else:
            return [match_.group(1), match_.group(2), None, None, None, None, None]
            # dd mm
    elif match_ := re.fullmatch('(\d{1,2})\s?[:;-]{1}\s?(\d{2})\s?[:;-]{1}\s?(\d{2})\s?[:;-]{1}\s?(\d{2})', str(data)): # hh:mm:ss:mm
        return [None, None, None, match_.group(1), match_.group(2), match_.group(3), match_.group(4)]
    elif match_ := re.match('(\d{1,2})\s?[:;-]{1}\s?(\d{2})\s?[:;-]{1}\s?(\d{2})', str(data)):                          # hh:mm:ss
        return [None, None, None, match_.group(1), match_.group(2), match_.group(3),  None]
    elif match_ := re.fullmatch('(\d{1,2})\s?[:;-]{1}\s?(\d{2})', str(data)):                                           # hh:mm
        return [None, None, None, match_.group(1), match_.group(2), None, None]




    else:
        print('No match')

    return


# print(parse_date('2023-3-30T5:1:5:50+0200'))


# # function which calculates the score
# def calc_score(data1, data2):
#     #define score for every type of connection
#     entire_match = timestamp_scoring.query('`scoring description` == "If there is a match by year, date, month, day, hours, minute, or second"')['score'][0]
#     year_month_day_minute = timestamp_scoring.query('`scoring description` == "If there is a match by year, date, month, day, minute"')['score'][1]
#     year_month_day_hour_up_to_5_mins = timestamp_scoring.query('`scoring description` == "if there is a match by year, date, month, day, hours, with a difference of up to 5 minutes"')['score'][2]
#     year_month_day_up_to_5_mins = timestamp_scoring.query('`scoring description` == "if there is a match by year, date, month, day, with a difference of up to 5 minutes"')['score'][3]
#     no_match = timestamp_scoring.query('`scoring description` == "else"')['score'][4]
#
#     score = no_match   # 0
#
#     mins_and_secs1 = int(data1[4]) * 60 + int(data1[5])
#     mins_and_secs2 = int(data2[4]) * 60 + int(data2[5])
#
#     # If there is a match by year, date, month, day, hours, minute, or second - score 100%
#     for i in range(7):
#         if data1[i] == data2[i]:
#             score = entire_match     # 100
#             continue
#         else:
#             score = no_match         # 0
#             break
#     if score == entire_match:        # if score == 100:
#         return score
#
#     # If there is a match by year, date, month, day, minute - score 95%
#     elif data1[0] == data2[0] and data1[1] == data2[1] and data1[2] == data2[2] and data1[4] == data2[4]:
#         score = year_month_day_minute      # 95
#
#     # if there is a match by year, date, month, day, hours, with a different of up to 5 minutes - 90%
#     elif data1[0] == data2[0] and data1[1] == data2[1] and data1[2] == data2[2] and data1[3] == data2[3] \
#                                                      and abs(mins_and_secs1-mins_and_secs2) <= 300:
#         score = year_month_day_hour_up_to_5_mins     # 90
#
#     # if there is a match by year, date, month, day, with a difference of up to 5 minutes - 70%
#     elif data1[0] == data2[0] and data1[1] == data2[1] and data1[2] == data2[2] \
#                                                      and abs(mins_and_secs1-mins_and_secs2) <= 300:
#         score = year_month_day_up_to_5_mins     # 70
#
#     return score
#
#
# # function which look for connection between two columns (take 2 columns and return dictionary with indexes of rows which have connection)
# def find_connections(column1, column2):
#     connection_indexes = {}
#     for i in range(max(len(column1), len(column2))):
#         connection_indexes[i] = []   # for each key we create list of values
#         for j in range(len(column2)):
#             date1 = parse_date(column1[i])   # parsed cell from 1 table
#             date2 = parse_date(column2[j])   # parsed cell from 2 table
#             score = calc_score(date1, date2)
#             # if score > 0 add indexes of rows form tables as key and value to dict
#             if score == 0:
#                 continue
#             else:
#                 connection_indexes[i].append([j, score])
#
#     # if one key has multiple values (to 1 row in 1 table we have 2+ connections from 2 table) it choose connection with highest score
#     for key in connection_indexes.keys():  # loop through all the keys
#         value = connection_indexes[key]
#         # print('key and value before', key, value)
#         if len(value) > 1:
#             scores = []
#             for k in range(len(value)):
#                 scores.append(value[k][1])
#             for k in range(len(value)):
#                 if value[k][1] == max(scores):
#                     connection_indexes[key] = value[k][0]
#
#     return connection_indexes
#
#
# # function which write connections and scores to new columns to both tables
# def write_connections(df1, sheet1, column1, df2, sheet2, column2):
#     # df1 and df2 - tables where search match
#     # sheet1 and sheet2 - sheets of tables where search match
#     # column1 and column2 - columns between which search match
#
#     # dict with connection indexes from both tables
#     connection_dict = find_connections(df1[column1], df2[column2])
#
#     # open both table and create columns
#     wb1 = openpyxl.load_workbook(filename=name_table_1, read_only=False)
#     ws1 = wb1[sheet1]
#     ws1.cell(row=1, column=width1 + 1).value = 'connection'
#     ws1.cell(row=1, column=width1 + 2).value = 'score'
#
#     wb2 = openpyxl.load_workbook(filename=name_table_2, read_only=False)
#     ws2 = wb2[sheet2]
#     ws2.cell(row=1, column=width2 + 1).value = 'connection'
#     ws2.cell(row=1, column=width2 + 2).value = 'score'
#
#     # calculate and write score and match
#     for i, j in connection_dict.items():
#
#         # if row i from 1 table has match with row j form 2 table:
#         if len(j) > 0:
#             ws1.cell(row=i + 2, column=width1 + 1).value = f'{name_table_2}_{str(j[0][0] + 2)}'
#             ws2.cell(row=j[0][0] + 2, column=width2 + 1).value = f'{name_table_1}_{str(i + 2)}'
#             # write score
#             ws1.cell(row=i + 2, column=width1 + 2).value = str(j[0][1]) + ' %'
#             ws2.cell(row=j[0][0] + 2, column=width2 + 2).value = str(j[0][1]) + ' %'
#
#     wb1.save(name_table_1)
#     wb2.save(name_table_2)
#
#
# # write score and record id to both tables
# write_connections(df1, sheet_name_1, df1_column, df2, sheet_name_2, df1_column)

