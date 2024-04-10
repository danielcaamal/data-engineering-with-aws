class SqlDataQualityQueries:    
    # Example {'check_sql': "xxx", 'expected_result': xxx , comparison:'>'}]
    data_quality_checks = [
      # Check for row to be greater than 0
      {'check_sql': "SELECT COUNT(*) FROM users", 'expected_result': 0 , 'comparison':'>'},
      {'check_sql': "SELECT COUNT(*) FROM songs", 'expected_result': 0 , 'comparison':'>'},
      {'check_sql': "SELECT COUNT(*) FROM artists", 'expected_result': 0 , 'comparison':'>'},
      {'check_sql': "SELECT COUNT(*) FROM time", 'expected_result': 0 , 'comparison':'>'},
      {'check_sql': "SELECT COUNT(*) FROM songplays", 'expected_result': 0 , 'comparison':'>'},
      # Check for null values in tables
      {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0 , 'comparison':'='},
      # Check for duplicates in tables
      {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IN (SELECT userid FROM users GROUP BY userid HAVING COUNT(*) > 1)", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IN (SELECT songid FROM songs GROUP BY songid HAVING COUNT(*) > 1)", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IN (SELECT artistid FROM artists GROUP BY artistid HAVING COUNT(*) > 1)", 'expected_result': 0 , 'comparison':'='},
      {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IN (SELECT start_time FROM time GROUP BY start_time HAVING COUNT(*) > 1)", 'expected_result': 0 , 'comparison':'='},
    ]