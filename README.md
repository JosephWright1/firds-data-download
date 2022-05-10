USAGE:
    python main.py "S3 BUCKET NAME"

- Pulls data from a static link
- Parses it to find the first file in the list that appears on the earliest date
- Downloads that file
- Unzips, parses and converts a static subset of columns to CSV
- Uploads the CSV to S3 bucket given as the argument