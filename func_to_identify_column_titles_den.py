import pandas as pd
import numpy as np
import re
import pygsheets
import pymysql

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def get_worksheets_data(filename, *worksheet_titles):
    # Connecting to the Google Sheets
    gc = pygsheets.authorize(service_file='evident-hexagon-384908-f03941412745.json')
    # opens a spreadsheet by its name
    spreadsheet = gc.open(filename)
    # Get a list of all worksheets in the spreadsheet
    worksheets = {}
    for title in worksheet_titles:
        worksheets[title] = spreadsheet.worksheet_by_title(title).get_as_df()
    return worksheets


def find_title_rows(filename, *worksheet_titles):
    worksheets = {}
    df = get_worksheets_data(filename, *worksheet_titles)
    for title in worksheet_titles:
        # fill all empty row with np.nan
        df[title].replace(r'^\s*$', np.nan, regex=True, inplace=True)
        # remove leading empty rows
        while pd.isna(df[title].iloc[0]).all():
            df[title] = df[title].iloc[1:]
        if '' in df[title].columns:
            # set column names to the first non-empty row
            df[title].columns = df[title].iloc[0]
            df[title] = df[title].iloc[1:]
        worksheets[title] = df[title].reset_index(drop=True)
        # print(f"{title} DataFrame:\n{worksheets[title]}\nIndex: {worksheets[title].index}\n\n")
    return worksheets


def categorize_titles(main_df, df_with_categorize):
    results = pd.DataFrame(columns=['Column Name', 'Category by title'])
    for col_name in main_df.columns:
        # Проходимося по всіх рядках таблиці Pivot Table 10
        for index, row in df_with_categorize.iterrows():
            # Шукаємо співпадіння в колонці title_name
            if col_name in str(row['title_name']):
                # Додаємо назву колонки і відповідну категорію в словник
                results = pd.concat([results, pd.DataFrame(
                    {'Column Name': col_name, 'Category by title': row['INTERFACE NAME']}, index=[0])],
                                    ignore_index=True)
                break  # Зупиняємо пошук, якщо знайдено співпадіння
    return results


def categorize_emails(df):
    results = pd.DataFrame(columns=['Column Name', 'Category by data'])
    for column in df.columns:
        emails = df.loc[df[column].apply(
            lambda x: isinstance(x, str) and bool(
                re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', x)
            )
        ), column]
        # emails.reset_index(drop=True)
        if not emails.empty:
            results = pd.concat(
                [results, pd.DataFrame({'Column Name': column, 'Category by data': 'Email'}, index=[0])],
                ignore_index=True)
        else:
            results = pd.concat([results, pd.DataFrame(
                {'Column Name': column, 'Category by data': 'no result'}, index=[0])],
                                ignore_index=True)
    return results


def categorize_trx_id(df):
    results = pd.DataFrame(columns=['Column Name', 'Category by data'])

    for i in range(len(df.columns)):
        list_of_result_for_each_cell = []
        flag = 0

        # if column is not unique, it`s not id
        if df.iloc[:, i].size != df.iloc[:, i].nunique():
            results = pd.concat(
                [results,
                 pd.DataFrame({'Column Name': df.columns[i], 'Category by data': 'no result'}, index=[0])],
                ignore_index=True)
            continue

        # for unique columns
        for j in range(len(df.index)):
            # if id has space, it is not id
            if re.search(r"[\s]", str(df.iloc[j][i])):
                list_of_result_for_each_cell = [0 for i in range(len(df.index))]

            # if column is not unique, it`s not id
            if re.search(r"@", str(df.iloc[j][i])):  # if it has @ sign, it can`t be a transaction id, skip it
                list_of_result_for_each_cell.append(0)
                flag = 0

            elif re.match(r"^[0-9]{23}$", str(df.iloc[j][i])):  # if it has 23 digits, it isn`t id, it`s ARN, skip it
                list_of_result_for_each_cell.append(0)
                flag = 0

            elif re.match(r"^[0-9]{1,9}$", str(df.iloc[j][i])):  # if it has LESS THAN 10, it isn`t id
                list_of_result_for_each_cell.append(0)
                flag = 0

            elif re.match(r"\B[A-Z]", str(df.iloc[j][i])) and not re.search(r"[\s]", str(
                    df.iloc[j][i])):  # if it has capital letters inside word and does not have spaces
                list_of_result_for_each_cell.append(1)
                flag = 1

            elif re.search(r"[A-Z]+[a-z]+[A-Z]+", str(df.iloc[j][i])) or re.search("[a-z]+[A-Z]+[a-z]+", str(
                    df.iloc[j][i])) and not re.search(r"[\s]", str(
                df.iloc[j][i])):  # if it has mix of 2 cases and does not have spaces
                list_of_result_for_each_cell.append(1)
                flag = 1

            elif re.match(r"^[^+][0-9]{9,}", str(df.iloc[j][i])) and \
                    re.match(r"^[^:]+$", str(df.iloc[j][i])) and not \
                    re.search(r"[\s]", str(df.iloc[j][i])):  # if more than 10 digits and no + and : sign and no spaces
                list_of_result_for_each_cell.append(1)
                flag = 1

            elif (re.search(r"([0-9]?[A-z]?)+", str(df.iloc[j][i])) or re.search(r"([A-z]?[0-9]?)+",
                                                                                 str(df.iloc[j][i]))) and re.match(
                r"^[^:]+$", str(df.iloc[j][i])) and not re.search(r"[\s]", str(
                df.iloc[j][i])):  # if it has mix of digits and letters and does not have : sign and spaces
                list_of_result_for_each_cell.append(1)
                flag = 1

            else:
                list_of_result_for_each_cell.append(0)
                flag = 0

        # number of data in column which detected as id
        appropriate_data = 0
        for k in list_of_result_for_each_cell:
            appropriate_data += k

        # if lest than 50% of data in column detected as not id, categorize column as no result
        if not len(list_of_result_for_each_cell) == 0:
            if appropriate_data / len(list_of_result_for_each_cell) < 0.5:
                results = pd.concat(
                    [results, pd.DataFrame({'Column Name': df.columns[i], 'Category by data': 'no result'}, index=[0])],
                    ignore_index=True)
            else:
                results = pd.concat(
                    [results,
                     pd.DataFrame({'Column Name': df.columns[i], 'Category by data': 'TRX ID'}, index=[0])],
                    ignore_index=True)
        else:
            results = pd.concat(
                [results, pd.DataFrame({'Column Name': df.columns[i], 'Category by data': 'no result'}, index=[0])],
                ignore_index=True)

    return results


def categorize_phone(df):
    results = pd.DataFrame(columns=['Column Name', 'Category by data'])
    for column in df.columns:
        phone = df.loc[df[column].apply(lambda x: isinstance(x, str) and bool(re.search(r'^\+\d+', x))), column]
        if not phone.empty:
            results = pd.concat([results, pd.DataFrame(
                {'Column Name': column, 'Category by data': 'Phone'}, index=[0])],
                                ignore_index=True)
        else:
            results = pd.concat([results, pd.DataFrame(
                {'Column Name': column, 'Category by data': 'no result'}, index=[0])],
                                ignore_index=True)
    return results


def calculate_final_output(results, column_type_score):
    results.loc[:, 'Final output'] = ''
    results.loc[:, 'Score'] = '0'

    # scores for each case
    full_match = column_type_score.query('`scoring API name` == "columntypematch 1"')['score'][0]
    output_1_X_output_2_Y = column_type_score.query('`scoring API name` == "columntypematch 2"')['score'][1]
    output_1_X_output_2_no = column_type_score.query('`scoring API name` == "columntypematch 3"')['score'][2]
    output_1_no_output_2_Y = column_type_score.query('`scoring API name` == "columntypematch 4"')['score'][3]
    output_1_no_output_2_no = column_type_score.query('`scoring API name` == "columntypematch 5"')['score'][4]
    for index, row in results.iterrows():
        if row['Category by title'] == row['Category by data']:
            results.at[index, 'Final output'] = row['Category by title']
            results.at[index, 'Score'] = full_match  # 100%
        elif row['Category by title'] == 'no result':
            results.at[index, 'Final output'] = row['Category by data']
            results.at[index, 'Score'] = output_1_no_output_2_Y    # 95 %
        elif row['Category by data'] == 'no result':
            results.at[index, 'Final output'] = row['Category by title']
            results.at[index, 'Score'] = output_1_X_output_2_no  # 90 %
        elif row['Category by title'] != row['Category by data']:
            results.at[index, 'Final output'] = row['Category by title']
            results.at[index, 'Score'] = output_1_X_output_2_Y   # 75 %
        else:
            results.at[index, 'Final output'] = 'empty'
            results.at[index, 'Score'] = output_1_no_output_2_no  # 0 %
    return results


# Функція, що об'єднує значення в колонці Category by data
def combine_categories(row):
    # Список категорій з усіх таблиць
    categories = [category for category in row.values if category != 'no result']
    # Якщо є хоча б одна категорія (Email або id), повертаємо її
    if categories:
        return categories[0]
    # Якщо немає жодної категорії, повертаємо 'no result'
    else:
        return 'no result'


def add_data_to_main_df(df, res):
    df.insert(loc=0, column='Categorize', value='')

    # додавання значення 'new_value' у першій колонці після всіх даних
    last_row_idx = df.iloc[-1].name
    # додавання пустих рядків в кінець датасету
    df.loc[len(df)] = [None] * len(df.columns)
    df.loc[len(df)] = [None] * len(df.columns)
    df.loc[len(df)] = [None] * len(df.columns)
    df.loc[len(df)] = [None] * len(df.columns)

    df.loc[last_row_idx + 1] = ['Category by title'] + [None] * (len(df.columns) - 1)
    df.loc[last_row_idx + 2] = ['Category by data'] + [None] * (len(df.columns) - 1)
    df.loc[last_row_idx + 3] = ['Final output'] + [None] * (len(df.columns) - 1)
    df.loc[last_row_idx + 4] = ['Score'] + [None] * (len(df.columns) - 1)

    for index, row in res.iterrows():
        col_name = row['Column Name']
        for column in res.columns[1:]:
            a = res.loc[res['Column Name'] == col_name, column].item()
            indx = df['Categorize'] == column
            df.loc[indx, col_name] = a
            df.to_csv('processed_file.csv', index=False)
    return df


# def add_data_to_main_df(df, res):
#     df.insert(loc=0, column='Categorize', value='')
#
#     # Add new rows to the DataFrame
#     last_row_idx = df.iloc[-1].name
#     df.loc[last_row_idx + 1] = ['Category by title'] + [None] * (len(df.columns) - 1)
#     df.loc[last_row_idx + 2] = ['Category by data'] + [None] * (len(df.columns) - 1)
#     df.loc[last_row_idx + 3] = ['Final output'] + [None] * (len(df.columns) - 1)
#     df.loc[last_row_idx + 4] = ['Score'] + [None] * (len(df.columns) - 1)
#     for index, row in res.iterrows():
#         col_name = row['Column Name']
#         for column in res.columns[1:]:
#             a = res.loc[res['Column Name'] == col_name, column].item()
#             indx = df['Categorize'] == column
#             df.loc[indx, col_name] = a
#
#     # Save only the new rows to a new file
#     new_rows = df.iloc[last_row_idx+1:]
#     new_filename = 'processed_file.csv'
#     new_rows.to_csv(new_filename, index=False)
#
#     return df


def categorize_columns(filename):
    try:
        df = pd.read_excel(f'{filename}')
    except ValueError:
        df = pd.read_csv(f'{filename}')
    # connecting to SQL cloud to scoring table
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
        sql1 = "SELECT * FROM Scoring"
        sql2 = 'SELECT * FROM Categorization_2'
        cursor.execute(sql1, ())
        cursor.execute(sql2, ())
        result = cursor.fetchone()
        scoring_df = pd.read_sql_query(sql1, conn)
        categor_df = pd.read_sql_query(sql2, conn)
        # print(scoring_df)

    # table with scores for column type
    column_type_score = timestamp_scoring = scoring_df.query('`Function name` == "columntype"').reset_index(drop=True)

    # cat = pd.read_excel(f'/home/{os.getlogin()}/file_upload/categorization/Data type recognision.xlsx',
    #                     sheet_name='Categorization')
    cat = categor_df

    result = categorize_titles(df, cat)
    result1 = categorize_emails(df)
    result2 = categorize_trx_id(df)

    merged_df = pd.merge(result1, result2, on='Column Name', how='outer')
    merged_df['Category by data'] = merged_df[['Category by data_x', 'Category by data_y']].apply(combine_categories,
                                                                                                  axis=1)
    merged_df.drop(['Category by data_x', 'Category by data_y'], axis=1, inplace=True)

    result3 = result.merge(merged_df, on='Column Name', how='inner')

    res = calculate_final_output(result3, column_type_score)
    final_data = add_data_to_main_df(df, res)
    return final_data

categorize_columns('ex.slxs')