#!/usr/bin/env python
# coding: utf-8
# Especifica configurações da aplicação, como o caminho para as pastas com os arquivos ZIP e arquivos de dados
# dados para abertura de banco de dados, configuração de e-mail e tipos de arquivos sendo processados
class cp:
    # Configuração de pastas e/ou buckets para a aplicação
    FILES_FOLDER       = "emb-dev-data-analytics-landing-zone-sb"
    AIRLINES_FOLDER    = ["Embraer/KLM/EGS/QAR/E2", "Embraer/RPA/EGS/QAR/E2"]
    FAILED_FOLDER      = "emb-dev-data-analytics-failed-zone-sb"
    PROCESSING_FOLDER  = "emb-dev-data-analytics-processing-zone-sb"
    RAW_FOLDER         = "emb-dev-data-analytics-raw-zone-sb"
    TEMP_FILES_FOLDER  = "temp"
    PROCESS_ZIP_FOLDER = "zip-files"
    
    # Configuracao do tipo de acesso
    ACCESS_TYPE        = "CSV" #CSV (arquivo CSV) ou DB (DynamoDB)
    FILE_HASH_NAME     = "file_hash_control.csv"
    FILE_LOG_NAME      = "file_log_control.csv"

    # Especifica o acesso ao banco de dados
    BD_NOME_BANCO    = "dynamodb"
    BD_REGION        = "us-east-1"

    # Configuração do envio de e-mail
    EMAIL_PORT          = 5704
    EMAIL_SERVER        = ""
    EMAIL_USER          = ""
    EMAIL_PASSWORD      = ""

    #tipos de arquivo
    MAIN_ZIP_FILE       = 1
    INTERNAL_ZIP_FILE   = 2
    PROCESS_FILE        = 3
    RAW_FILE            = 4

# Criação da classe zf com funções genéricas
# imports necessários para a execução
import os
from datetime import datetime as dt
import datetime as dt2
import random as rd
import hashlib as hash
import boto3
from boto3.dynamodb.conditions import Key
import zipfile
import csv
import io

class zf:
    # Funcao para obter a data e a hora atuais
    def getDateTime(mask = "%Y%m%d_%H%M%S"):
        dtRet = dt.now().strftime(mask)
        return dtRet
    # Funcao para obter numeros aleatorios por uma quantidade "n" de vezes
    def getRandomNumber(numTimes = 3):
        numRet = ""
        for i in range(numTimes):
            numRet = numRet + str(rd.randint(0, 9))
        return numRet
    # Função para obter o hash de um arquivo
    def getHashFromData(data):
        sha512hash = ""
        sha512hash = hash.sha512(data).hexdigest()
        return sha512hash
    # Função para obter a conexao com o banco de dados
    def getDBConnection(serviceName = "", regionName = ""):
        # Define os valores para acesso ao servidor de banco de dados
        if (serviceName == ""):
            serviceName = "dynamodb"
        if (regionName == ""):
             regionName = "us-east-1"
        #Conectando ao servidor
        db  = boto3.resource(serviceName, regionName)
        return db
    # Função para pesquisar uma determinada condição e retornar a quantidade de linhas
    def getDBRows(dbName, tableName, columnName, theValue):
        table = dbName.Table(tableName)
        response = table.query(
            KeyConditionExpression=Key(columnName).eq(theValue)
        )
        return len(response['Items'])
    # Função para pesquisar uma determinada condição e retornar os dados
    def getDBData(dbName, tableName, columnName, theValue):
        table = dbName.dynamodb.Table(tableName)

        response = table.query(
            KeyConditionExpression=Key(columnName).eq(theValue)
        )
        return response['Items']
    # Função para pesquisar uma determinada condição e retornar os dados
    def query_item(table_name, column_name, item_to_find):
        # Conexão com o DynamoDB
        dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
        # Selecione a tabela
        #table_name = "file_hash_control"
        table = dynamodb.Table(table_name)
        response = table.query(KeyConditionExpression=Key(column).eq(item_to_find))
        return response['Items']
    # Função para inserir dados em tabelas do DynamoDB
    def setDBRow(dbName, tableName, theData):
        table = dbName.dynamodb.Table(tableName)
        response = table.put_item(
           Item = theData
        )
        return response
    # Criação da função descompactar_em_outro_bucket
    def descompactar_em_outro_bucket(bucket_origem, caminho_zip, bucket_destino, prefixo_destino=""):
        s3 = boto3.client('s3')
        # 1. Carregue o arquivo ZIP do bucket origem para a memória.
        zip_obj = s3.get_object(Bucket=bucket_origem, Key=caminho_zip)
        with io.BytesIO(zip_obj['Body'].read()) as zip_file:
            with zipfile.ZipFile(zip_file) as zip_ref:
                # 2. Descompacte o arquivo ZIP na memória.
                for nome_arquivo in zip_ref.namelist():
                    with zip_ref.open(nome_arquivo) as file:
                        data = file.read()
                        # 3. Faça o upload de cada arquivo descompactado para o bucket de destino.
                        s3.put_object(Bucket=bucket_destino, Key=f"{prefixo_destino}{nome_arquivo}", Body=data)
    # Criação da função validar_zip
    def validar_zip(bucket_origem, caminho_zip):
        s3 = boto3.client('s3')
        # 1. Carregue o arquivo ZIP do bucket origem para a memória.
        zip_obj = s3.get_object(Bucket=bucket_origem, Key=caminho_zip)
        try:
            retVal = None
            with io.BytesIO(zip_obj['Body'].read()) as zip_file:
                with zipfile.ZipFile(zip_file) as zip_ref:
                    retVal = zip_ref.testzip()
                # end with
            # end with
        except Exception as e: 
            retVal = str(e)
        # end try
        return retVal
    # end def
    def append_to_csv_s3(bucket_name, file_name, data):
        """
        Append data to a CSV file stored in S3.
        :param bucket_name: String. The name of the S3 bucket.
        :param file_name: String. The CSV file name.
        :param data: List of tuples. The data to append (each tuple is a row).
        """
        # Inicializa o cliente do S3
        s3_client = boto3.client('s3')
        # Verifica se o arquivo existe no S3
        try:
            # Tenta buscar o objeto do S3
            s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_name)
            existing_data = s3_object['Body'].read().decode('utf-8')
        except s3_client.exceptions.NoSuchKey:
            existing_data = ''
        # end if
        # Utiliza StringIO para simular um arquivo
        csv_file = io.StringIO()
        csv_file.write(existing_data)
        csv_writer = csv.writer(csv_file)

        # Faz o append dos dados no arquivo
        csv_writer.writerows(data)

        # Move o cursor para o início do arquivo
        csv_file.seek(0)

        # Faz o upload do arquivo atualizado para o S3
        s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_file.getvalue())
        csv_file.close()
    def search_string_in_s3_file(bucket, key, string):
        # Inicializa o cliente do S3
        s3_client = boto3.client('s3')
        # variáveis de controle de execução e de retorno
        count_lines = 0
        line_ret = 0
        try:
            # Obtenha o objeto do S3
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            # Leia o conteúdo do arquivo
            contents = obj['Body'].read().decode('utf-8')
            # Itere sobre cada linha do arquivo
            for line in contents.splitlines():
                # Verifique se a string de pesquisa está na linha atual
                count_lines = (count_lines + 1)
                if string in line:
                    line_ret = count_lines
                    break
                    #return line  # Retorna a linha inteira que contém a string
        except s3_client.exceptions.NoSuchKey:
            line_ret = 0
        return line_ret
# Fluxo de execução da transferência de arquivos da landing zone para a zona de processamento

# imports necessários para a execução
import os
import zipfile
from pathlib import Path
import shutil
import boto3

# Função para verificar o conteúdo da pasta de arquivos definida
'''
Append data to a CSV file stored in S3.

:param bucket_name: String. The name of the S3 bucket.
:param file_name: String. The CSV file name.
:param data: List of tuples. The data to append (each tuple is a row).
'''
def verificar_pasta_de_arquivos(bd, caminho, nivel, s3, pasta_filha):
    
    #Configurações para processamento
    pasta_arquivos = cp.FILES_FOLDER
    if (nivel == 2):
        pasta_arquivos = cp.PROCESSING_FOLDER
    # end if
    pasta_processamento = cp.PROCESSING_FOLDER
    pasta_erro = cp.FAILED_FOLDER

    # obtém os dados da comanhia aérea e do tipo de arquivo
    aCaminho     = caminho.split(os.path.sep)
    ciaAerea     = aCaminho[1]
    origem       = aCaminho[2]
    tipoArquivo  = aCaminho[3]
    tipoAeronave = aCaminho[4]

    # define o caminho padrão de arquivos
    caminho_arquivos = pasta_arquivos + os.path.sep + caminho

    # define o caminho padrão de arquivos de processamento
    caminho_processamento = pasta_processamento + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho

    if (nivel == 2):
        caminho_arquivos = pasta_arquivos +           os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha
        caminho_processamento = pasta_processamento + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha
    # end if

    # define o caminho padrão de arquivos de erro
    caminho_erro = pasta_erro + os.path.sep + caminho

    # Lista os arquivos da pasta/diretório para verificar os que estão aptos para descompactação
    bucket_execucao = s3.Bucket(pasta_arquivos)
    cont_arquivos = 0
    for arquivo_zip_base in bucket_execucao.objects.all():
        if (caminho in arquivo_zip_base.key):

            cont_arquivos = (cont_arquivos + 1)

            arquivo_a_processar = os.path.basename(arquivo_zip_base.key)

            # Obtém o arquivo para verificação
            arquivo_zip = caminho_arquivos + os.path.sep + arquivo_a_processar

            arquivo_sucesso = caminho_processamento + os.path.sep + arquivo_a_processar

            arquivo_erro = caminho_erro + os.path.sep + arquivo_a_processar

            if (arquivo_a_processar.lower().endswith('.zip') or arquivo_a_processar.lower().endswith('.7z')):
                caminho_origem = caminho + os.path.sep + arquivo_a_processar
                caminho_destino = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho_origem
                origem  = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
                destino = pasta_processamento + os.path.sep + caminho_destino
                if (cp.ACCESS_TYPE == "DB"):
                    verificar_arquivo_zip(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, arquivo_zip_base.key)
                else:
                    verificar_arquivo_zip_csv(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, arquivo_zip_base.key)
                # end if 
            # end if
        # end if
    # end for

    bucket_execucao = s3.Bucket(pasta_processamento)
    cont_arquivos = 0
    for arquivo_zip_base in bucket_execucao.objects.all():
        if (caminho in arquivo_zip_base.key):

            cont_arquivos = (cont_arquivos + 1)

            arquivo_a_processar = os.path.basename(arquivo_zip_base.key)

            # Obtém o arquivo para verificação
            arquivo_zip = caminho_arquivos + os.path.sep + arquivo_a_processar

            arquivo_sucesso = caminho_processamento + os.path.sep + arquivo_a_processar

            arquivo_erro = caminho_erro + os.path.sep + arquivo_a_processar

            if ((not arquivo_a_processar.lower().endswith('.zip')) and (not arquivo_a_processar.lower().endswith('.7z'))):
                caminho_origem = caminho + os.path.sep + arquivo_a_processar
                caminho_destino = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho_origem
                origem  = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
                destino = pasta_processamento + os.path.sep + caminho_destino
                if (cp.ACCESS_TYPE == "DB"):
                    verificar_arquivo(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_processamento, arquivo_zip_base.key)
                else:
                    verificar_arquivo_csv(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_processamento, arquivo_zip_base.key)
# Função para validação dos arquivos .zip
def verificar_arquivo(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, chave_arquivo):
    #Configurações para processamento
    pasta_processamento = cp.PROCESSING_FOLDER
    pasta_erro = cp.FAILED_FOLDER
    
    # obtém os dados da comanhia aérea e do tipo de arquivo
    aCaminho     = caminho.split(os.path.sep)
    ciaAerea     = aCaminho[1]
    origem       = aCaminho[2]
    tipoArquivo  = aCaminho[3]
    tipoAeronave = aCaminho[4]

    try:
        # Define a validação como OK
        arquivo_ok = True
        erro_arquivo = ""
        
        # Obtém o nome base (Sem o caminho)
        nomebase = arquivo_a_processar

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório

        # Verifica se o arquivo já existe
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if

        # Configura o caminho para processamento do arquivo
        caminho_arquivo_processamento = chave_arquivo

        # Obtém o conteúdo do arquivo
        obj = s3.Object(pasta_arquivos, caminho_arquivo_processamento)
        dados = obj.get()['Body'].read()
        s = str(dados).encode('utf-8')

        # Obtém o hash do arquivo a partir do conteúdo
        hash_arquivo = zf.getHashFromData(s)

        # tratamento do nome para uma eventual exceção de arquivo
        arquivo_exc = arquivo_zip

        # Verifica a duplicidade do arquivo
        hash_pesquisa = "P03_" + hash_arquivo + "_" + ciaAerea + "_" + origem + "_" + tipoArquivo + "_" + tipoAeronave
        linhas = zf.getDBRows(db, "file_hash_control", "hash", hash_pesquisa)
        if (linhas == 0):
            dados_insert = {'hash':{'S': hash_pesquisa},
                            'data_hora':{'S': zf.getDateTime("%Y-%m-%d %H:%M:%S")},
                            'nome_arquivo_original':{'S': arquivo_a_processar},
                            'nome_arquivo_hash':{'S': arquivo_a_processar},
                            'origem':{'S': origem},
                            'status':{'S': "PROCESSADO"},
                            'detalhe':{'S': ""}
                           }
            zf.setDBRow(db, "file_hash_control", dados_insert)
        else:
            pass
        # end if
    except Exception as e: 
        erro_arquivo = str(e)
        arquivo_ok = False
    finally:
        pass
# Função para validação dos arquivos .zip
def verificar_arquivo_csv(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, chave_arquivo):
    #Configurações para processamento
    pasta_processamento = cp.PROCESSING_FOLDER
    pasta_erro = cp.FAILED_FOLDER
    
    arquivo_log  = cp.FILE_LOG_NAME #"logs_" + zf.getDateTime("%Y%m%d") + "_01.csv"
    arquivo_hash = cp.FILE_HASH_NAME #"hashs_" + zf.getDateTime("%Y%m%d") + "_01.csv"
    
    # obtém os dados da comanhia aérea e do tipo de arquivo
    aCaminho     = caminho.split(os.path.sep)
    ciaAerea     = aCaminho[1]
    origem       = aCaminho[2]
    tipoArquivo  = aCaminho[3]
    tipoAeronave = aCaminho[4]

    try:
        # Define a validação como OK
        arquivo_ok = True
        erro_arquivo = ""
        
        # Obtém o nome base (Sem o caminho)
        nomebase = arquivo_a_processar

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório

        # Verifica se o arquivo já existe
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if

        # Configura o caminho para processamento do arquivo
        caminho_arquivo_processamento = chave_arquivo

        # Obtém o conteúdo do arquivo
        obj = s3.Object(pasta_arquivos, caminho_arquivo_processamento)
        dados = obj.get()['Body'].read()
        s = str(dados).encode('utf-8')

        # Obtém o hash do arquivo a partir do conteúdo
        hash_arquivo = zf.getHashFromData(s)

        # tratamento do nome para uma eventual exceção de arquivo
        arquivo_exc = arquivo_zip

        # Verifica a duplicidade do arquivo
        hash_pesquisa = "P03_" + hash_arquivo + "_" + ciaAerea + "_" + origem + "_" + tipoArquivo + "_" + tipoAeronave

        linhas = zf.search_string_in_s3_file(cp.FILES_FOLDER, arquivo_hash, hash_pesquisa)
        if (linhas == 0):
            zf.append_to_csv_s3(cp.FILES_FOLDER, arquivo_hash, [(hash_pesquisa, zf.getDateTime(), arquivo_a_processar, arquivo_a_processar)])
        else:
            pass
        # end if
    except Exception as e: 
        erro_arquivo = str(e)
        arquivo_ok = False
    finally:
        pass
    # end try
# end def

# Função para validação dos arquivos .zip
def verificar_arquivo_zip(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, chave_arquivo):
    
    #Configurações para processamento
    pasta_processamento = cp.PROCESSING_FOLDER
    pasta_erro = cp.FAILED_FOLDER
    
    # obtém os dados da comanhia aérea e do tipo de arquivo
    aCaminho     = caminho.split(os.path.sep)
    ciaAerea     = aCaminho[1]
    tipoArquivo  = aCaminho[3]
    origem       = aCaminho[2]
    tipoArquivo  = aCaminho[3]
    tipoAeronave = aCaminho[4]
    
    try:
        # Define a validação como OK
        arquivo_ok = True
        erro_arquivo = ""
        
        # Obtém o nome base (Sem o caminho)
        nomebase = arquivo_a_processar

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if
        
        # Obtém o conteúdo do arquivo
        caminho_arquivo_processamento = caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            caminho_arquivo_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if
        
        obj = s3.Object(pasta_arquivos, caminho_arquivo_processamento)
        dados = obj.get()['Body'].read()
        s = str(dados).encode('utf-8')

        # Obtém o hash do arquivo
        hash_arquivo = zf.getHashFromData(s)

        # tratamento do nome para uma eventual exceção de arquivo
        arquivo_exc = arquivo_zip

        # Verifica a duplicidade do arquivo
        hash_pesquisa = "P0" + str(nivel) + "_" + hash_arquivo + "_" + ciaAerea + "_" + origem + "_" + tipoArquivo + "_" + tipoAeronave

        linhas = zf.getDBRows(db, "file_hash_control", "hash", hash_pesquisa)
        if (linhas == 0):
            if (zf.validar_zip(pasta_arquivos, caminho_arquivo_processamento) == None):

                aArquivo = os.path.splitext(arquivo_a_processar)
                caminho_zip_s3 = caminho + os.path.sep + arquivo_a_processar
                caminho_pasta_zip_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep +  aArquivo[0] + os.path.sep
                if (nivel == 2):
                    caminho_zip_s3 =                  cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
                    caminho_pasta_zip_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + aArquivo[0] + os.path.sep
                # end if
                zf.descompactar_em_outro_bucket(pasta_arquivos, caminho_zip_s3, pasta_processamento, caminho_pasta_zip_processamento)
                dados_insert = {'hash':{'S': hash_pesquisa},
                                'data_hora':{'S': zf.getDateTime("%Y-%m-%d %H:%M:%S")},
                                'nome_arquivo_original':{'S': arquivo_a_processar},
                                'nome_arquivo_hash':{'S': arquivo_a_processar},
                                'origem':{'S': origem},
                                'status':{'S': "PROCESSADO"},
                                'detalhe':{'S': ""}
                               }
                zf.setDBRow(db, "file_hash_control", dados_insert)
                if (nivel == 1):
                    verificar_pasta_de_arquivos(bd, caminho, cp.INTERNAL_ZIP_FILE, s3, aArquivo[0])
                # end if
                if (nivel == 2):
                    #arquivo_exc = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + nomebase
                    s3.delete_object(Bucket=pasta_arquivos, Key=caminho_zip_s3)  # Apagar o arquivo da pasta após descompactar
                # end if
            else:
                erro_arquivo = "Arquivo corrompido ou inválido"
                arquivo_ok = False
            # end if
        else:
            pass
        # end if
    except zipfile.BadZipFile:
        erro_arquivo = "Arquivo corrompido ou inválido"
        arquivo_ok = False
    except FileNotFoundError:
        erro_arquivo = "Arquivo não encontrado"
        #arquivo_ok = False
    except Exception as e: 
        erro_arquivo = str(e)
        arquivo_ok = False
    finally:
        if (not arquivo_ok):
            aArquivo = os.path.splitext(arquivo_a_processar)
            novo_arquivo_com_falha = aArquivo[0] + "_" + zf.getDateTime() + "_" + zf.getRandomNumber(10) + aArquivo[1]
            dados_insert = {'hash':{'S': hash_pesquisa},
                            'data_hora':{'S': zf.getDateTime("%Y-%m-%d %H:%M:%S")},
                            'nome_arquivo_original':{'S': arquivo_a_processar},
                            'nome_arquivo_log':{'S': novo_arquivo_com_falha},
                            'origem':{'S': origem},
                            'status':{'S': "ERRO"},
                            'detalhe':{'S': erro_arquivo},
                            'envio_alerta':{'S': "N"}
                           }
            zf.setDBRow(db, "file_log_control", dados_insert)
            #zf.append_to_csv_s3(cp.FILES_FOLDER, arquivo_log, [(hash_pesquisa, zf.getDateTime(), arquivo_a_processar, novo_arquivo_com_falha, "ERR")])
            s3.Object(pasta_erro, caminho + "/" + novo_arquivo_com_falha).copy_from(CopySource={'Bucket': pasta_arquivos, 'Key': caminho_arquivo_processamento})
            s3.Object(pasta_arquivos,caminho_arquivo_processamento).delete()
# Função para validação dos arquivos .zip
def verificar_arquivo_zip_csv(arquivo_a_processar, bd, nivel, caminho, s3, pasta_filha, pasta_arquivos, chave_arquivo):
    
    #Configurações para processamento
    pasta_processamento = cp.PROCESSING_FOLDER
    pasta_erro = cp.FAILED_FOLDER
    
    arquivo_hash = cp.FILE_HASH_NAME
    arquivo_log  = cp.FILE_LOG_NAME
    
    # obtém os dados da comanhia aérea e do tipo de arquivo
    aCaminho     = caminho.split(os.path.sep)
    ciaAerea     = aCaminho[1]
    tipoArquivo  = aCaminho[3]
    origem       = aCaminho[2]
    tipoArquivo  = aCaminho[3]
    tipoAeronave = aCaminho[4]
    
    try:
        # Define a validação como OK
        arquivo_ok = True
        erro_arquivo = ""
        
        # Obtém o nome base (Sem o caminho)
        nomebase = arquivo_a_processar

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if

        # Obtém o nome simples do arquivo (sem extensão) para criação de pasta/diretório
        arquivo_zip = pasta_arquivos + os.path.sep + caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            arquivo_zip = pasta_arquivos + os.path.sep + cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if
        
        # Obtém o conteúdo do arquivo
        caminho_arquivo_processamento = caminho + os.path.sep + arquivo_a_processar
        if (nivel == 2):
            caminho_arquivo_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
        # end if
        
        obj = s3.Object(pasta_arquivos, caminho_arquivo_processamento)
        dados = obj.get()['Body'].read()
        s = str(dados).encode('utf-8')

        # Obtém o hash do arquivo
        hash_arquivo = zf.getHashFromData(s)

        # tratamento do nome para uma eventual exceção de arquivo
        arquivo_exc = arquivo_zip

        # Verifica a duplicidade do arquivo
        hash_pesquisa = "P0" + str(nivel) + "_" + hash_arquivo + "_" + ciaAerea + "_" + origem + "_" + tipoArquivo + "_" + tipoAeronave

        linhas = zf.search_string_in_s3_file(cp.FILES_FOLDER, arquivo_hash, hash_pesquisa)

        if (linhas == 0):
            if (zf.validar_zip(pasta_arquivos, caminho_arquivo_processamento) == None):

                aArquivo = os.path.splitext(arquivo_a_processar)
                caminho_zip_s3 = caminho + os.path.sep + arquivo_a_processar
                caminho_pasta_zip_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep +  aArquivo[0] + os.path.sep
                if (nivel == 2):
                    caminho_zip_s3 =                  cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + arquivo_a_processar
                    caminho_pasta_zip_processamento = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + aArquivo[0] + os.path.sep
                # end if
                zf.descompactar_em_outro_bucket(pasta_arquivos, caminho_zip_s3, pasta_processamento, caminho_pasta_zip_processamento)
                zf.append_to_csv_s3(cp.FILES_FOLDER, arquivo_hash, [(hash_pesquisa, zf.getDateTime(), arquivo_a_processar, arquivo_a_processar)])
                if (nivel == 1):
                    verificar_pasta_de_arquivos(bd, caminho, cp.INTERNAL_ZIP_FILE, s3, aArquivo[0])
                # end if
                if (nivel == 2):
                    #arquivo_exc = cp.PROCESS_ZIP_FOLDER + os.path.sep + caminho + os.path.sep + pasta_filha + os.path.sep + nomebase
                    s3.delete_object(Bucket=pasta_arquivos, Key=caminho_zip_s3)  # Apagar o arquivo da pasta após descompactar
                # end if
            else:
                erro_arquivo = "Arquivo corrompido ou inválido"
                arquivo_ok = False
            # end if
        else:
            pass
        # end if
    except zipfile.BadZipFile:
        erro_arquivo = "Arquivo corrompido ou inválido"
        arquivo_ok = False
    except FileNotFoundError:
        erro_arquivo = "Arquivo não encontrado"
        #arquivo_ok = False
    except Exception as e: 
        erro_arquivo = str(e)
        arquivo_ok = False
    finally:
        if (not arquivo_ok):
            aArquivo = os.path.splitext(arquivo_a_processar)
            novo_arquivo_com_falha = aArquivo[0] + "_" + zf.getDateTime() + "_" + zf.getRandomNumber(10) + aArquivo[1]
            zf.append_to_csv_s3(cp.FILES_FOLDER, arquivo_log, [(hash_pesquisa, zf.getDateTime(), arquivo_a_processar, novo_arquivo_com_falha, "ERR")])
            s3.Object(pasta_erro, caminho + "/" + novo_arquivo_com_falha).copy_from(CopySource={'Bucket': pasta_arquivos, 'Key': caminho_arquivo_processamento})
            s3.Object(pasta_arquivos,caminho_arquivo_processamento).delete()
# Função principal (sub main())
def main():
    print("\nTransferencia para processing zone - inicio " + zf.getDateTime())
    s3 = boto3.resource("s3")
    
    caminhos = cp.AIRLINES_FOLDER

    bd = zf.getDBConnection(cp.BD_NOME_BANCO, cp.BD_REGION)
    for caminho in caminhos:
        verificar_pasta_de_arquivos(bd, caminho, cp.MAIN_ZIP_FILE, s3, "")
    # end for
    print("Transferencia para processing zone - fim " + zf.getDateTime())
# Instancia o script
#if (__name__ == "__main__"):
main()
