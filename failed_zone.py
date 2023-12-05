import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# Inicialize o cliente S3
s3_client = boto3.client('s3')

bucket_name = 'emb-dev-data-analytics-failed-zone'

# Define os caminhos para listar os arquivos
paths = [
    'Embraer/KLM/EGS/QAR/E2/',
    'Embraer/RPA/EGS/QAR/E2/'
]
prefixo='E2-'
# Função para listar os arquivos em um determinado caminho no bucket S3
def listar_arquivos(bucket, prefix):
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    arquivos = []
    for page in page_iterator:
        if "Contents" in page:
            arquivos.extend([obj['Key'] for obj in page['Contents']])
    return arquivos

# Função para determinar a empresa de aviação com base no path
def determinar_empresa(path):
    return path.split('/')[1]  # Os três primeiros caracteres após 'Embraer/'

# Função para determinar o tipo de aeronave com base no nome do arquivo
def determinar_tipo_aeronave(nome_arquivo):
    if 'ARCHIVE_EOF' in nome_arquivo:
        #print(nome_arquivo)
        prefixo = "E2-"  # Prefixo para todos os tipos de aeronaves
        tipo_aeronave = nome_arquivo[31:34]  # Três caracteres a partir da posição 54
        return f"{prefixo}{tipo_aeronave}"

# Função para determinar o tipo de arquivo com base no nome do arquivo
def determinar_tipo_arquivo(nome_arquivo):
    if "ARCHIVE_EOF" in nome_arquivo:
        return "ARCHIVE_EOF"
    elif "TCRF" in nome_arquivo:
        return "TCRF"
    elif "ACRF" in nome_arquivo:
        return "ACRF"
    elif "ACMF" in nome_arquivo:
        return "ACMF"
    else:
        return "Desconhecido"

# Lista para guardar os resultados
relatorio = []

# Dicionários para manter contagens de arquivos por empresa, tipo de aeronave e tipo de arquivo
contagem_por_empresa = {}
contagem_por_tipo_aeronave = {}
contagem_por_tipo_arquivo = {}

# Lista os arquivos para cada caminho e adiciona ao relatório
for path in paths:
    arquivos = listar_arquivos(bucket_name, path)
    empresa = determinar_empresa(path)
    #print(arquivo)
    for arquivo in arquivos:
        tipo_aeronave = determinar_tipo_aeronave(os.path.basename(arquivo))
        tipo_arquivo = determinar_tipo_arquivo(os.path.basename(arquivo))

        # Atualize as contagens
        contagem_por_empresa[empresa] = contagem_por_empresa.get(empresa, 0) + 1
        if 'ARCHIVE_EOF' in arquivo:
            contagem_por_tipo_aeronave[tipo_aeronave] = contagem_por_tipo_aeronave.get(tipo_aeronave, 0) + 1
        if tipo_arquivo != 'Desconhecido':
            contagem_por_tipo_arquivo[tipo_arquivo] = contagem_por_tipo_arquivo.get(tipo_arquivo, 0) + 1

        # Move os arquivos para a pasta ALERTED
        novo_caminho = 'ALERTED/' + os.path.join(os.path.dirname(arquivo), os.path.basename(arquivo))
        s3_client.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': arquivo}, Key=novo_caminho)
        #s3_client.delete_object(Bucket=bucket_name, Key=arquivo)

# Cria o relatório em formato de texto
texto_relatorio = f"Relatório de Arquivos S3:\n\n"
texto_relatorio += "Quantidade de arquivos por empresa:\n"
for empresa, quantidade in contagem_por_empresa.items():
    texto_relatorio += f"{empresa}: {quantidade}\n"

texto_relatorio += "\nQuantidade de arquivos por tipo de aeronave:\n"
for tipo_aeronave, quantidade in contagem_por_tipo_aeronave.items():
    texto_relatorio += f"{tipo_aeronave}: {quantidade}\n"

texto_relatorio += "\nQuantidade de arquivos por tipo de arquivo:\n"
for tipo_arquivo, quantidade in contagem_por_tipo_arquivo.items():
    texto_relatorio += f"{tipo_arquivo}: {quantidade}\n"


from botocore.exceptions import ClientError

# Função para enviar e-mail utilizando o Amazon SES
def enviar_email_via_ses(destinatario, assunto, corpo):
    # Crie um novo cliente SES no ponto AWS de sua preferência
    client = boto3.client('ses', region_name='us-east-1')

    try:
        # Envie o e-mail
        response = client.send_email(
            Source='email_remetente@example.com',
            Destination={
                'ToAddresses': [
                    destinatario,
                ],
            },
            Message={
                'Subject': {
                    'Data': assunto,
                    'Charset': 'UTF-8'
                },
                'Body': {
                    'Text': {
                        'Data': corpo,
                        'Charset': 'UTF-8'
                    },
                },
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("E-mail enviado! Message ID:"),
        print(response['MessageId'])

# Agora, você pode chamar a função enviar_email_via_ses no lugar do pseudo-código anterior
enviar_email_via_ses(destinatario='email_destino', assunto='Relatório de Arquivos S3 Corrompidos Na Zona FAILED', body=texto_relatorio)



# Enviar relatório por email (usando a função enviar_email_via_ses que você forneceu)
#enviar_email_via_ses(destinatario='email_destino', assunto='Relatório de Arquivos S3 Corrompidos Na Zona FAILED', corpo=texto_relatorio)

print(texto_relatorio)
