import logging
import requests
import re
import boto3

from sys import exit
from typing import Tuple, Union, List, Dict
from time import sleep
from xml.etree import ElementTree
from io import StringIO, BytesIO
from zipfile import ZipFile
from csv import DictWriter

from Constants import URL, TARGET_ELEMENTS


def get_latest_file(retry: bool=False) -> Union[str, None]:
    """Retrieves the latest date from the URL provided in the assignment document
    
    Args:
        retry (bool): True if second attempt, False otherwise
    
    Returns:
        target: the URL of the latest file
    """
    try:
        r = requests.get(URL)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to retrieve latest file from {URL}\n{str(e)}"
        if retry:
            logging.error(msg)
            exit()
        else:
            logging.warning(msg + ", retrying")
            sleep(10)
            return get_latest_file(retry=True)

    tree = ElementTree.parse(StringIO(r.text))
    del r

    file_date_pattern = r"DLTINS_(.*?)_.*?\.zip"
    min_date = float('inf')
    target = None
    docs = tree.findall(".//doc")

    for doc in docs:
        file_info = {
            x.attrib.get("name"): x.text 
            for x in doc.findall(".//str")
        }
        
        match = re.match(file_date_pattern, file_info["file_name"]) # group 0 is full string, group 1 is date
        date_int = int(match.group(1)) # only require int for comparison in context
        
        # if the date is the lower, update target
        if date_int < min_date:
            target = file_info["download_link"]
            min_date = date_int
        
    return target


def get_data(target: str, retry: bool=False) -> Tuple[List[Dict[str, str]], str]:
    """Get the zipped data from the 
    target URL and parse it into a list 
    of dictionaries that represent each record

    Args:
        target (str): the URL of the zipped xml file to be downloaded
        retry (bool): True if second attempt, False otherwise

    Returns:
        Tuple[list[dict[str, str]], str]: a list of dictionaries 
                                          representing records
                                          and the last filename
    """
    try:
        zf = ZipFile(BytesIO(requests.get(target).content))
        bad_file = zf.testzip()
        if bad_file:
            raise Exception(f"Invalid file in FIRDS zip {bad_file}")
        logging.info("FIRDS data validated")
    except requests.RequestException as e:
        msg = f"Failed to retrieve file from {target}\n{str(e)}"
        if retry:
            logging.error(msg)
            exit()
        else:
            logging.warning(msg + ", retrying")
            sleep(10)
            return get_data(target, retry=True)
    except Exception as e:
        msg = f"Failed to retrieve file from {target}\n{str(e)}"
        logging.error(msg)
        exit()

    output = []

    for file in zf.filelist:
        xml = zf.read(file).decode("u8")
        records = re.findall(r"<FinInstrm>.*?</FinInstrm>", xml) # get all records using regex for speed
        while records:
            tree = ElementTree.parse(StringIO(records.pop())) # make tree of each record for mem efficiency
            output.append({
                alias: tree.find(element).text 
                for alias, element 
                in TARGET_ELEMENTS.items()
            })
    
    return output, file.filename


def write_to_csv(output: List[Dict[str, str]]) -> StringIO:
    """Convert a list of dictionaries 
       into a CSV format in memory
       and return the CSV data 
       as a StringIO object

    Args:
        output (list[dict[str, str]]): the list of dictionaries
                                       representing the records

    Returns:
        StringIO: the file object representation 
                  of the CSV data in memory
    """
    outf = StringIO()
    
    writer = DictWriter(
        outf, 
        TARGET_ELEMENTS.keys()
    )
    writer.writeheader()
    
    while output:
        try:
            row = output.pop()
            writer.writerow(row)

        except Exception as e:
            msg = f"Failed to write row:\n{row}\n{str(e)}"
            logging.error(msg)

    outf.seek(0)

    return outf

def upload_to_s3(outf: StringIO, bucket: str, file_name: str) -> str:
    """Upload the file object "outf" to AWS S3

    Args:
        outf (StringIO): the file object representing the data
        bucket (str): AWS S3 bucket name
        file_name (str): the file name, used as the S3 key
    """
    s3 = boto3.client("s3")
    try:
        s3.put_object(
            Body=outf.read().encode("u8"),
            Bucket=bucket,
            Key=file_name
        )
    except Exception as e:
        msg = f"failed to write file '{str(file_name)}' to s3: {str(e)}"
        logging.error(msg)
        
    return