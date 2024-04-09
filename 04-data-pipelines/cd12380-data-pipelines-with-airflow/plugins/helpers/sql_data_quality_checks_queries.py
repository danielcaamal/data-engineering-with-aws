class SqlDataQualityQueries:
    # Count rows in tables
    count_rows = [
        "SELECT COUNT(*) FROM users",
        "SELECT COUNT(*) FROM songs",
        "SELECT COUNT(*) FROM artists",
        "SELECT COUNT(*) FROM time",
        "SELECT COUNT(*) FROM songplays",
    ]

    # Check for null values in tables
    check_nulls = [
        "SELECT COUNT(*) FROM users WHERE userid IS NULL",
        "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
        "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
        "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
        "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
    ]

    # Check for duplicates in tables
    check_duplicates = [
        "SELECT COUNT(*) FROM users WHERE userid IN (SELECT userid FROM users GROUP BY userid HAVING COUNT(*) > 1)",
        "SELECT COUNT(*) FROM songs WHERE songid IN (SELECT songid FROM songs GROUP BY songid HAVING COUNT(*) > 1)",
        "SELECT COUNT(*) FROM artists WHERE artistid IN (SELECT artistid FROM artists GROUP BY artistid HAVING COUNT(*) > 1)",
        "SELECT COUNT(*) FROM time WHERE start_time IN (SELECT start_time FROM time GROUP BY start_time HAVING COUNT(*) > 1)",
        "SELECT COUNT(*) FROM songplays WHERE playid IN (SELECT playid FROM songplays GROUP BY playid HAVING COUNT(*) > 1)",
    ]