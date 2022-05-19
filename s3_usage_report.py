import boto3
from datetime import datetime
import statistics
import csv
import os
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

def fetch_previous_months():
now = datetime.now().strftime('%m')
month = int(now)
if month > 2:
    return range(month - 2,month + 1)
elif month == 1:
    return [11,12,1]
else:
    return [12,1,2]

def find_average(object_size_list):
size_average = statistics.mean(object_size_list)
return size_average

def fetch_bucket_size(resp,month):
obj_size_list = []
for obj in resp:
    obj_month = int(obj['LastModified'].strftime('%m'))
    if month == obj_month:
        obj_size_list.append(obj['Size'])
if len(obj_size_list) != 0:
    average_bucket_size = find_average(obj_size_list)
    return average_bucket_size
else:
    return 0

def lambda_handler(event, context):
s3_client = boto3.client('s3')
resp =s3_client.list_objects(Bucket=os.environ['bucket'])['Contents']
month_list = fetch_previous_months()
filename = key = "S3-Usage_"+datetime.now().strftime("%d%m%Y%H%M%S") + ".csv"
header_csv = ['MONTH','AVERAGE_BUCKET_SIZE']
fo=open("/tmp/"+filename,"a",newline='')
csv_w = csv.writer(fo)
current_date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
heading = "S3 Usage - "+os.environ['bucket']
date_heading = "Current Date - "+current_date
csv_w.writerow([heading])
csv_w.writerow([date_heading])
csv_w.writerow([])
csv_w.writerow(header_csv)
for month in month_list:
    bucket_size = fetch_bucket_size(resp,month)
    datetime_object = datetime.strptime(str(month), "%m")
    month_name = datetime_object.strftime("%B")
    bucket_size_tb = bucket_size/1099511992568
    csv_w.writerow([month_name,bucket_size_tb])
fo.close()
client = boto3.client('ses')
message = MIMEMultipart()
message['Subject'] = heading
message['From'] = os.environ['FROM_EMAIL']
message['To'] = ', '.join(os.environ['TO_EMAIL'].split(','))
part = MIMEText('Please find the attached CSV File, For Bucket quarantine-prod-us-east-1', 'html')
message.attach(part)
part = MIMEApplication(open("/tmp/"+filename, 'rb').read())
part.add_header('Content-Disposition', 'attachment', filename=filename)
message.attach(part)
response = client.send_raw_email(
Source=message['From'],
Destinations= os.environ['TO_EMAIL'].split(','),
RawMessage={
    'Data': message.as_string()
}
)
