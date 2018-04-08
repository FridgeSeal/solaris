import pandas
import sys
import uuid
import sqlite3
import math as maths


def handle_args():
    # TODO: proper argument handling, defaults and fallbacks
    return sys.argv[1]


def read_file(fpath):
    return pandas.read_csv(fpath)


def handle_db(db_path='solaris.db'):
    return sqlite3.connect(db_path)


def pull_all_db(db_conn, first, last):
    query = """SELECT FirstName, MiddleName, LastName, DateOfBirth, PlaceOfBirth, 
                Address, Position, ACN, UUID
                FROM personnel_data
                WHERE FirstName=:first
                    AND LastName=:last"""
    return pandas.read_sql(query, con=db_conn, params={'first': first, 'last': last})


def nan_filter(x):
    if isinstance(x, str):
        return True
    elif maths.isnan(x):
        return False


def n_users_filter(x):
    if x[1] > 1:
        return True
    else:
        return False


def check_homogenous(data):
    # Might need to add middle name here?
    n_dob = data['DateOfBirth'].nunique(dropna=True)
    n_pob = data['PlaceOfBirth'].nunique(dropna=True)
    n_addr = data['Address'].nunique(dropna=True)
    vals = [('DateOfBirth', n_dob), ('PlaceOfBirth', n_pob), ('Address', n_addr)]
    return list(filter(n_users_filter, vals))


def extract_present(data):
    restricted_cols = ['UserID', 'Position', 'ACN']
    cols = [x for x in data.columns if x not in restricted_cols]
    return {x: data[x].unique() for x in cols}


def split_frames(data):
    """
    Used for partitioning a df into separate user-subsets when multiple dob's/addr/pob's etc are detected
    :param data: a pandas DataFrame
    :return: a list of cleaned dataframes that contain data for single person only ("homogenous")
    """
    # Let's just pick dob, just because.
    n_dob = data['DateOfBirth'].unique()
    # TODO: alternative approach that looks for and uses a variable in the subset that will ensure overlap
    # i.e. prevent the potential problem of "orphan" date's
    results = []
    for dob in filter(nan_filter, n_dob):
        temp = data[data['DateOfBirth'] == dob]  # We're making the implicit assumption that there'll be at least 1
        # DoB value in the data set
        avail = extract_present(temp)
        beta = data[
                    (data['DateOfBirth'] == dob) |
                    (data['MiddleName'] == avail['MiddleName'][0]) |  # Also making the assumption that we get single
                    (data['PlaceOfBirth'] == avail['PlaceOfBirth'][0]) |  # values back here, because this currently
                    (data['Address'] == avail['Address'][0])  # silently drops any succeeding ones...
                    ]
        results.append(beta)
    return results


def generate_uuid():
    return str(uuid.uuid4())


def unpack(value):
    res = list(filter(nan_filter, value))
    if len(res) == 0:
        return 'NULL'
    else:
        return res[0]


def collapse_records(data):
    """
    Takes a "homogenous" dataset (that might contain NA's for some variables) and transforms it into 2 dataframes:
    The first is a tuple/single row df that contains all the unified information
    The 2nd is a dataframe that contains the position column, ACN column and a user ID column.
    If the User ID column isn't found, one is created and added
    :param data:
    :return: A tuple of the form: (userdata, companydata dataframe)
    """
    gen_id = False
    fname = data['FirstName'].unique()[0]
    lname = data['LastName'].unique()[0]
    mname = unpack(data['MiddleName'].unique())
    dbirth = unpack(data['DateOfBirth'].unique())
    plbirth = unpack(data['PlaceOfBirth'].unique())
    addr = unpack(data['Address'].unique())
    company_data = data[['Position', 'ACN']]
    if 'UUID' in data.columns:  # This should be present because we'll be merging in a sql db that will have it
        uuid_val = unpack(data['UUID'].unique())  # But it doesn't hurt to check
        if uuid_val == 'NULL':
            gen_id = True
    else:
        gen_id = True
    if gen_id:
        uuid_val = generate_uuid()
    company_data = company_data.assign(UUID=uuid_val)
    user_data = {'FirstName': fname, 'LastName': lname, 'MiddleName': mname, 'DateOfBirth': dbirth,
                 'PlaceOfBirth': plbirth, 'Address': addr, 'UUID': uuid_val}
    return user_data, company_data


def clean_tables(db_conn, company_df):
    qry_cust = """SELECT T1.FirstName, T1.LastName, T1.MiddleName, T1.DateOfBirth, T1.PlaceOfBirth, T1.Address,
     T1.UUID 
     INTO personnel_data
     FROM personnel_chrono AS T1 
     WHERE
        T1.timestamp = (SELECT MAX(T2.timestamp) FROM personnel_chrono AS T2 WHERE T2.UUID = T1.UUID)"""
    company_qry = """SELECT UUID, Position, ACN FROM company_data"""
    rem_tbl = "DELETE FROM personnel_data"
    company_temp = pandas.read_sql(company_qry, con=db_conn)
    company_temp = company_temp.append(company_df)
    company_temp = company_temp.drop_duplicates()
    company_temp.to_sql('company_data', con=db_conn, index=False, if_exists='replace')
    cursor = db_conn.cursor()
    cursor.execute(rem_tbl)
    cursor.execute(qry_cust)
    db_conn.commit()


def main():
    filename = handle_args()
    df = read_file(filename)
    connection = handle_db()
    all_personel = []
    all_company = []
    for fname in df['FirstName'].unique():
        temp = df[df['FirstName'] == fname]
        for lname in temp['LastName'].unique():
            temp2 = temp[temp['LastName'] == lname]
            sql_data = pull_all_db(connection, fname, lname)
            combined = temp2.append(sql_data)
            temp3 = split_frames(combined)
            deduplicated = [collapse_records(dataset) for dataset in temp3]
            all_personel.extend([x[0] for x in deduplicated])
            all_company.extend([x[1] for x in deduplicated])
    personnel_df = pandas.DataFrame.from_dict(all_personel)
    company_df = pandas.concat(all_company)
    personnel_df.to_sql('personnel_chrono', con=connection, if_exists='append', index=False)
    clean_tables(connection, company_df)


if __name__ == '__main__':
    main()
