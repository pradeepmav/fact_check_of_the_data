# fact_check_of_the_data
This python script using "pola-rs" library provides at one shot, an excel sheet having facts of your data (polars data frame). It contains all pivotal statistical descriptions of all columns needed for Machine Learning / Data Science projects namely., data type, number of non-missing , number of missing, missing percentage, distinct, min, max, mean, median, mode in sheet for quick understanding/ interpretation purposes.

required libraries  -  pola-rs, requests, and xlsxwriter

Usage:

import requests;
exec(requests.get('https://raw.githubusercontent.com/pradeepmav/fact_check_of_the_data/aeacfa4123c0f826c5c6ecf060f05907e68f1612/FCOTD.py').text);
fact_check_of_the_data(any/yourpolarsdataframe)

![image](https://github.com/user-attachments/assets/a010a223-f0d1-416d-866b-fd3dadde04b6)
