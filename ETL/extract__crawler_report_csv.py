import requests
import os

current_dir = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(current_dir)
local_filename = os.path.join(parent_dir, 'out', 'crawler_report.csv.gz')
url = 'https://stat-ds-test.s3.amazonaws.com/getstat_com_serp_report_201707.csv.gz'

def download_file(url):
    # local_filename = 'out/crawler_report.csv.gz'
    r = requests.get(url, stream=True)
    with open(local_filename, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk: # filter out keep-alive new chunks
                f.write(chunk)
                #f.flush() commented by recommendation from J.F.Sebastian
    return local_filename

download_file(url);
