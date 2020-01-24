
import boto3
from urllib.request import urlopen
import re
import csv
from io import (StringIO, BytesIO)
from pydash import set_

def get_s3():
    try:
        s3 = boto3.client('s3')
        return (True, s3, None)
    except Exception as e:
        return (False, None, e)

def download_benner_file():
    try:
        data = urlopen('http://echo.jpl.nasa.gov/~lance/delta_v/delta_v.rendezvous.html').read()
        return (True, data, None)
    except Exception as e:
        return (False, None, e)

def split_lines(text):
    try:
        lines = text.splitlines()
        return (True, lines, None)
    except Exception as e:
        return (False, None, e)

def parse_to_csv(lines):
    try:
        r = re.compile((
            '\s*(?P<rank>\d+)'
            '\s+(?P<percentile>\d+\.\d+)'
            '\s+(?P<name>\(\d+\)(\s+[-\w ]+)?)?'
            '\s+(?P<pdes1>\d+)'
            '\s+(?P<pdes2>[-\w]+)'
            '\s+(?P<deltav>\d+\.\d+)'
            '\s+(?P<h>\d+\.\d+)'
            '\s+(?P<a>\d+\.\d+)'
            '\s+(?P<e>\d+\.\d+)'
            '\s+(?P<i>\d+\.\d+)'))
        fields = ('pdes', 'dv', 'H', 'a', 'e', 'i')
        f = StringIO()

        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()

        c = 0
        for line in lines:
            c+=1
            if c < 4:
                continue

            m = r.match(line.decode("utf-8"))
            if not m:
                continue

            writer.writerow({
                'pdes': ('%s %s' % (m.group('pdes1'), m.group('pdes2'))).strip(),
                'dv': m.group('deltav'),
                'H': m.group('h'),
                'a': m.group('a'),
                'e': m.group('e'),
                'i': m.group('i')
                })
        return (True, f.getvalue().encode('utf-8'), None)

    except Exception as e:
        return (False, None, e)

def head_bucket(s3, bucket_name):
    try:
        result = s3.head_bucket(Bucket=bucket_name)
        return (True, result, None)
    except Exception as e:
        return (False, None, e)

def put_object(s3, bucket, filename, data):
    try:
        result = s3.put_object(Bucket=bucket, Key=filename, Body=data)
        return (True, result, None)
    except Exception as e:
        return (False, None, e)

def get_benner_filename():
    return 'benner_deltav.csv'

def handler(event):
    print('Getting S3...')
    ok, s3, error = get_s3()
    if ok == False:
        print("Can't get an s3 client:", error)
        raise error

    print('Heading bucket...')
    ok, _, error = head_bucket(s3, event['bucket'])
    if ok == False:
        print("Can't head a bucket:", error)
        raise error
    
    print('Downlodaing benner file...')
    ok, data, error = download_benner_file()
    if(ok == False):
        print("download_benner_file error:", error)
        return

    print('Splitting lines...')
    ok, lines, error = split_lines(data)
    if(ok == False):
        print("split_lines error:", error)
        return

    print('Parsing to CSV...')
    ok, data, error = parse_to_csv(lines)
    if(ok == False):
        print("write_csv error:", error)
        return

    print('Putting CSV on S3...')
    ok, _, error = put_object(s3, event['bucket'], get_benner_filename(), data)
    if ok == False:
        print("put object failed:", error)
        raise error
    
    updated_event = set_(event, 'benner', get_benner_filename())
    print("Done! event:", updated_event)
    return updated_event

if __name__ == "__main__":
   handler({'tableName': 'asteroids', 'bucket': 'asteroid-files'})