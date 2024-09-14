
import pandas as pd
from sqlalchemy import create_engine
import datetime
import logging


#Logging setup
logger = logging.getLogger()

logging.basicConfig(filename='data_processing.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#inläsning av CSV-filer, och skapande av data frame med önskade kolumner

def read_weather_data(file_path_1, file_path_2):
    try:
        data_1 = pd.read_csv(file_path_1, delimiter=';')
        data_2 = pd.read_csv(file_path_2, delimiter=';')
        
        if 'Representativt dygn' in data_1.columns and 'Representativt dygn' in data_2.columns:
            df = pd.merge(data_1, data_2, on='Representativt dygn', how='left')
        else:
            logger.error("Both files must contain the 'Representativt dygn' column.")
            raise ValueError("Missing 'Representativt dygn' column in one of the files.")
        
        df = df[['Representativt dygn', 'Lufttemperatur', 'Nederbördsmängd']]

        logger.info(f"CSV files '{file_path_1}' and '{file_path_2}' read and merged successfully.")
        return df

    except FileNotFoundError as fnf_error:
        logger.error(f"File not found: {fnf_error}")
        raise
    except pd.errors.ParserError as parse_error:
        logger.error(f"Error parsing CSV file: {parse_error}")
        raise
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        raise

#Funktion för att hantera datan så att den matas in korrekt
def format_and_clean_data(df):
    for index, row in df.iterrows():
        if isinstance(row['Representativt dygn'], str):
            if '-' in row['Representativt dygn']:
                try:
                    y, m, d = [int(x) for x in row['Representativt dygn'].split('-')]
                    new_date = datetime.date(y, m, d)
                except Exception as e:
                    logger.error(f"Error parsing date {row['Representativt dygn']}: {e}")
                    new_date = pd.NaT
            elif '/' in row['Representativt dygn']:
                try:
                    y, m, d = [int(x) for x in row['Representativt dygn'].split('/')]
                    new_date = datetime.date(y, m, d)
                except Exception as e:
                    logger.error(f"Error parsing date {row['Representativt dygn']}: {e}")
                    new_date = pd.NaT
            else:
                logger.error(f"Could not parse date {row['Representativt dygn']}")
                new_date = pd.NaT
        else:
            logger.error(f"Date value is not a string: {row['Representativt dygn']}")
            new_date = pd.NaT

        df.at[index, 'Representativt dygn'] = new_date

        # Hrocessa temperatur och nederbörd
        try:
            new_temp = float(str(row['Lufttemperatur']).replace(',', '.'))
        except ValueError:
            new_temp = pd.NA
        except Exception as e:
            logger.error(f"Error parsing temperature for row {index}: {e}")
            new_temp = pd.NA

        try:
            new_rain = float(str(row['Nederbördsmängd']).replace(',', '.'))
        except ValueError:
            new_rain = pd.NA
        except Exception as e:
            logger.error(f"Error parsing rainfall for row {index}: {e}")
            new_rain = pd.NA

        df.at[index, 'Lufttemperatur'] = new_temp
        df.at[index, 'Nederbördsmängd'] = new_rain

    return df

#funktion för att uppdatera SQL
def sql_updater(sql_path, df, table_name):
    try:
        engine = create_engine(sql_path)
        logger.info("Database connection successfully established")
        
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logger.info(f"Data successfully inserted into {table_name} table")

    except Exception as e:
        logger.error(f"Error during SQL operations: {e}")
        raise

# etablera main så att detta endast exekveras när scriptet är huvudmodul
if __name__ == "__main__":
    df = read_weather_data('smhi_temp.csv', 'smhi_nederbord.csv')
    sql_url = 'mssql://DESKTOP-1PS8E8C/weather?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
    format_and_clean_data(df)
    sql_updater(sql_url, df, 'jägersro')



