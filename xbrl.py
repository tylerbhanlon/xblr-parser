
from time import sleep
import requests
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import argparse
import openpyxl
from tqdm import tqdm 
import re
import pyfiglet

global year
global quarter
global email


def get_full_xbrl_data(df):
    result_indices = []
    print(f"\033[32mSearching 10-K forms for matching tag values\033[0m")  # Green text
 
    tmp_downloaded_filename = "null"
    target_text = "true</dei:DocumentFinStmtErrorCorrectionFlag>"
    auditor_target_text = "</dei:AuditorName"
    try:
        length = len(df)
        for index, row in tqdm(df.iterrows(), total=length, desc=f"Digging through XBRL data", leave=True):
            tqdm.write(f"Processing file: {index}/{length}")
            file_url = row['File Name']
            file_url = 'https://www.sec.gov/Archives/' + file_url

            response = requests.get(file_url, headers={"User-Agent" : email})
            response.raise_for_status()

            #Save temp file
            tmp_downloaded_filename = "tmpfile.xml"
            with open(f"./temp/{tmp_downloaded_filename}", 'wb') as f:
                f.write(response.content)

            f.close()

            #Read downloaded file and parse the content
            with open("./temp/" + tmp_downloaded_filename, 'r', encoding='utf-8') as f:
                file_content = f.read()
                if target_text in file_content:
                    # print(f"Found the text in file: {file_url}")
                    result_indices.append(index)

                    # Get the auditor name:
                    auditor_pos = file_content.find(auditor_target_text)

                    last_carrot = file_content.rfind('>', auditor_pos - 50, auditor_pos)

                    if last_carrot != -1:
                        # print text
                        auditor = file_content[last_carrot + 1:auditor_pos].strip()
                        df.at[index, 'Auditor'] = auditor
                        # df['Auditor'] = df.apply(auditor, axis=1)
                    continue
                # else:
                    # print(f"Text not found in file: {file_url}")
        # Filter the DataFrame to only include the rows with matching indices
        df_filtered = df[df.index.isin(result_indices)]
        return df_filtered
    
    except requests.exceptions.RequestException as e:
        print(f"Failed to download {file_url}: {e}")

    finally:
        # Delete the temporary file to free memory
        if os.path.exists("./temp/" + tmp_downloaded_filename):
            os.remove("./temp/" + tmp_downloaded_filename)
            print(f"\033[32mDeleted temporary file: ./temp/{tmp_downloaded_filename}\033[0m")  # Green text



def create_and_parse_dataframe():
    print(f"\033[32mCleaning and Parsing all XBRL data\033[0m")  # Green text
    headers = ['CIK', 'Company Name', 'Form Type', 'Date Filed', 'File Name']
    df = pd.read_csv('./temp/xbrl.idx', delimiter='|', names=headers, header=None, skiprows=10)

    # Filter the DataFrame to keep only rows where 'Form Type' is '10-K'
    df = df[df['Form Type'] == '10-K']

    # Reset the index so the row numbers are consecutive
    df.reset_index(drop=True, inplace=True)

    size = df.shape[0]
    print(f"\033[32mFound a total of {size} 10-K forms\033[0m")  # Green text
    
    filtered_df = get_full_xbrl_data(df)

    output_file = f"xbrl-{year}-{quarter}.xlsx"
    filtered_df.to_excel(output_file, index=False)

def get_xbrl_data():
    print(f"\033[32mGetting all XBRL Data for {year} {quarter}\033[0m")  # Green text
    # Make the API call and get the JSON response
    headers = {'User-Agent': email, 'Accept-Encoding': 'gzip,deflate', 'Host': 'www.sec.gov'}
    # endpoint = "https://www.sec.gov/Archives/edgar/full-index/2024/QTR4/form.idx"
    endpoint = f"https://www.sec.gov/Archives/edgar/full-index/{year}/{quarter}/xbrl.idx"

    response = requests.get(endpoint, headers=headers)

    if response.status_code == 200:
        # Save to a temporary file
        os.makedirs('./temp', exist_ok=True)
        with open('./temp/xbrl.idx', 'wb') as f:
            f.write(response.content)
            f.close()
        
        create_and_parse_dataframe()


if __name__ == "__main__":
    print(pyfiglet.figlet_format("XBRL PARSER"))
    print("\033[32mWritten by Tyler Hanlon January 2025\033[0m")
    sleep(2)
    print('\n\n\n\n\n\n=============================================')

    parser = argparse.ArgumentParser(description="Process year and quarter to get forms")
    parser.add_argument('--year', type=str, help="Financial year of documents you need in XXXX format (ex: 2024)")
    parser.add_argument('--quarter', type=str, help="Financial quarter in QTRX format (ex: QTR4)")
    parser.add_argument('--email', type=str, help="Email to attach to our requests (mandated by SEC to track requests)")

    args = parser.parse_args()

    if args.year == None:
        print("\033[31mNo argument --year supplied. Use the following syntax: \n\033[0m")
        print("\033[32mpython xbrl.py --year 2024 --quarter QTR3 --email youname@email.com\n\033[0m")
    elif args.quarter == None:
        print("\033[31mNo argument --quarter supplied. Use the following syntax: \n\033[0m")
        print("\033[32mpython xbrl.py --year 2024 --quarter QTR3 --email youname@email.com\n\033[0m")
    elif args.email == None:
        print("\033[31mNo argument --email supplied. Use the following syntax: \n\033[0m")
        print("\033[32mpython xbrl.py --year 2024 --quarter QTR3 --email youname@email.com\n\033[0m")
    else:
        year = args.year
        quarter = args.quarter
        email = args.email
        get_xbrl_data()
  