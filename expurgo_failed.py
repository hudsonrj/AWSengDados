import boto3
from datetime import datetime, timedelta
import pytz
import smtplib
from email.mime.text import MIMEText

# Configurações da AWS e SMTP
aws_access_key_id = 'YOUR_ACCESS_KEY'
aws_secret_access_key = 'YOUR_SECRET_KEY'
bucket_name = 'emb-dev-data-analytics-failed-zone-sb'
paths = ["Embraer/KLM/EGS/QAR/E2/", "Embraer/RPA/EGS/QAR/E2/"]
smtp_server = 'smtp.example.com'
smtp_port = 587
smtp_user = 'your@email.com'
smtp_password = 'yourpassword'
recipient_email = 'recipient@email.com'

# Conectar ao S3
s3 = boto3.client('s3')

# Função para listar arquivos
def list_files(bucket, path):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=path)
    return response.get('Contents', [])

# Função para filtrar arquivos antigos
def filter_old_files(files):
    utc = pytz.UTC
    three_months_ago = datetime.now(utc) - timedelta(days=0)
    return [file for file in files if file['LastModified'] < three_months_ago]

# Função para excluir arquivos
def delete_files(bucket, files):
    deleted_files = []
    for file in files:
        if 'zip' in file['Key'] or 'ZIP' in file['Key']:
            print(f"Excluindo arquivo: {file['Key']}")
            #s3.delete_object(Bucket=bucket, Key=file['Key'])
            deleted_files.append(file['Key'])
    return deleted_files

# Função para enviar email
def send_email(deleted_files_info):
    body = f"Data da Exclusão: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    for path, files in deleted_files_info.items():
        body += f"Path: {path}\nQuantidade de Arquivos: {len(files)}\nLista de Arquivos: {', '.join(files)}\n\n"
    
    msg = MIMEText(body)
    msg['Subject'] = 'Relatório de Exclusão de Arquivos'
    msg['From'] = smtp_user
    msg['To'] = recipient_email

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_password)
        server.sendmail(smtp_user, recipient_email, msg.as_string())

# Processo principal
deleted_files_info = {}
for path in paths:
    files = list_files(bucket_name, path)
    old_files = filter_old_files(files)
    deleted_files = delete_files(bucket_name, old_files)
    deleted_files_info[path] = deleted_files

#send_email(deleted_files_info)
