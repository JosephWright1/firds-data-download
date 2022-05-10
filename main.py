import logging

from traceback import format_exc
from datetime import datetime as dt

from Utils import get_latest_file, \
get_data, write_to_csv, upload_to_s3


logging.basicConfig(filename='main.log', level=logging.DEBUG) 


"""
    NOTES:
        - I am going to make the code nice, but not exactly PEP-8.
          There is a VSC extension for auto PEP-8, but rigid PEP-8
          compliance can end up quite ugly. in the PEP-8 docs they
          do note that it is just a guide, and sometimes other
          formatting options will make more sense.
"""


def put_first_firds_file_in_s3(bucket):
    target = get_latest_file()
    if not target:
        logging.warning("No target file found in ESMA file request response")
        return
    
    output, file_name = get_data(target)
    if not output:
        logging.warning("No data found in target file")
        return
    
    outf = write_to_csv(output)


    response = upload_to_s3(outf, bucket, file_name)


if __name__ == "__main__":
    try:
        logging.info(f"Beginning execution at {dt.now()}")
        
        from sys import argv

        if len(argv) != 2:
            msg = "Invalid command line arguments. " \
                  "Expecting S3 bucket name ONLY"
            raise Exception(msg)
        else:
            bucket = argv[1]

        put_first_firds_file_in_s3(bucket)

        logging.info(f"Execution completed at {dt.now()}")
    
    except:
        logging.error(format_exc())