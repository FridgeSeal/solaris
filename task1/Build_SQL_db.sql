CREATE TABLE company_data
    ( UUID TEXT NOT NULL,
      Position TEXT NOT NULL,
      ACN INTEGER NOT NULL
     );

CREATE TABLE personnel_chrono (
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    MiddleName TEXT,
    DateOfBirth TEXT,
    PlaceOfBirth TEXT,
    Address TEXT,
    Position TEXT NOT NULL,
    ACN INTEGER NOT NULL,
    UUID TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
    );

CREATE TABLE personnel_data (
    FirstName TEXT NOT NULL,
    LastName TEXT NOT NULL,
    MiddleName TEXT,
    DateOfBirth TEXT,
    PlaceOfBirth TEXT,
    Address TEXT,
    Position TEXT NOT NULL,
    ACN INTEGER NOT NULL,
    UUID TEXT);