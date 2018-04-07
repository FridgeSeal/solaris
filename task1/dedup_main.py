import pandas
import sys
import uuid
import sqlite3
import math as maths


def handle_args(arg_list):
    return sys.argv[1]


def read_file(fpath):
    return pandas.read_csv(fpath)


def query_db(query):
    print('Stub')


def nan_filter(x):
    if isinstance(x, str):
        return True
    elif maths.isnan(x):
        return False


def unpack(value):
    res =  list(filter(nan_filter, value))
    if len(res) == 0:
        return 'nan'  # Consider swapping to 'NULL' so SQLite automatically recognises it
    else:
        return res[0]


def collapse_records(data):
    """
    Takes a "homogenous" dataset (that might contain NA's for some variables) and transforms it into 2 dataframes:
    The first is a tuple/single row df that contains all the unified information
    The 2nd is a dataframe that contains the position column, ACN column and a user ID column.
    If the User ID column isn't found, one is created and added
    :param data:
    :return: A tuple of the form: (userdata, positionACN dataframe)
    """
    fname = data['FirstName'].unique()[0]
    lname = data['LastName'].unique()[0]
    mname = unpack(data['MiddleName'].unique())
    dbirth = unpack(data['DateOfBirth'].unique())
    plbirth = unpack(data['PlaceOfBirth'].unique())
    addr = unpack(data['Address'].unique())


def split_frames(data):
    """
    Used for partitioning a df into separate user-subsets when multiple dob's/addr/pob's etc are detected
    :param data:
    :return:
    """
    print('Logic goes here')
    # Detect which of date of birth, place of birth or address contain the multiple values
    # Split along uniqueness lines of that variable
    # Check if all 3 candidate variables report unity
    # recurse into splitting along misbehaving variable until we achieve n homogenous datasets
    # return a tuple of frames


def main():  # TODO: turn this into some nice, functional style functions.
    filename = handle_args(sys.argv)
    df = read_file(filename)
    for fname in df['FirstName'].unique():
        temp = df[df['FirstName'] == fname]
        for lname in temp['LastName'].unique():
            temp2 = temp[temp['LastName'] == lname]
            # Execute call to db for any matching records here
            # Merge the results and then proceed as usual
            n_dob = temp2['DateOfBirth'].nunique(dropna=True)
            n_pob = temp2['PlaceOfBirth'].nunique(dropna=True)
            n_addr = temp2['Address'].nunique(dropna=True)
            if (n_dob > 1 ) or (n_pob > 1) or (n_addr > 1):  # This is the case where we have people sharing a name
                # spl = split_frames(temp2)
                # Figure out how to rejoin main flow from here
                print('We got one!')
            else:  # This is the case where a subset is nicely behaved and contains just one person's details
                collapse_records(temp2)
                # We should have the same sort of structure as from the other if arm by this point
            # Append/update records here
            # Rather than mutating records directly, would probably be better to use the append-only model
    # Then execute a query/job on the db that builds a query table out of the latest results for each user
    #  We can then safely exit


if __name__ == '__main__':
    main()
