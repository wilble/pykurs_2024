
import pytest
import pandas as pd
import datetime
from unittest.mock import patch
from kunskapskontroll_2_wb import read_weather_data, format_and_clean_data


# Test för funktionen att läsa in 2 CSV-filer till en dataframe
@patch('kunskapskontroll_2_wb.pd.read_csv')
def test_read_weather_data(mock_read_csv):
    
    df_temp = pd.DataFrame({
        'Representativt dygn': ['2023-01-01', '2023-01-02'],
        'Lufttemperatur': [15.2, 16.4]
    })
    
    df_rain = pd.DataFrame({
        'Representativt dygn': ['2023-01-01', '2023-01-02'],
        'Nederbördsmängd': [1.2, 0.4]
    })

    mock_read_csv.side_effect = [df_temp, df_rain]

    df = read_weather_data('temp.csv', 'rain.csv')

    assert len(df) == 2
    assert 'Lufttemperatur' in df.columns
    assert 'Nederbördsmängd' in df.columns
    assert df.iloc[0]['Lufttemperatur'] == 15.2
    assert df.iloc[0]['Nederbördsmängd'] == 1.2


# Test för andra funktionen
def test_format_and_clean_data():
    
    df = pd.DataFrame({
        'Representativt dygn': ['2023/01/01', '2023-01-02'],
        'Lufttemperatur': ['15,2', '16.4'],
        'Nederbördsmängd': ['1,2', '0.4']
    })

    
    cleaned_df = format_and_clean_data(df)

  
    assert cleaned_df.iloc[0]['Representativt dygn'] == datetime.date(2023, 1, 1)
    assert cleaned_df.iloc[0]['Lufttemperatur'] == 15.2
    assert cleaned_df.iloc[0]['Nederbördsmängd'] == 1.2

df = pd.DataFrame({
    'Representativt dygn': ['2023-01-01', '2023/01/02'],
    'Lufttemperatur': ['15,2', '16.4'],
    'Nederbördsmängd': ['1,2', '0.4']
})

