README para Script de Transferência AWS S3

Transferencia para RAW

Sobre o Script
Este script Python automatiza a transferência de arquivos do bucket de processamento (processing zone) para o bucket de dados brutos (raw zone) na AWS S3, garantindo que apenas arquivos não duplicados e íntegros sejam movidos.

Requisitos
Python 3.x
AWS CLI configurado com permissões adequadas
Biblioteca boto3 instalada (pip install boto3)
Configuração
Defina as variáveis na classe cp para corresponder aos seus buckets e caminhos do S3.
Insira as informações de configuração de e-mail se desejar receber notificações para erros.
Instalação
Clone o repositório ou faça o download do script para o seu ambiente local. Não é necessária nenhuma instalação adicional, desde que os requisitos acima sejam atendidos.

Uso
Execute o script a partir da linha de comando:

bash
Copy code
python nome_do_script.py

Funcionalidades
Validação de integridade de arquivos ZIP
Evita a duplicação de arquivos com base em hash SHA-512
Transferência automatizada de arquivos para o bucket raw
Registra operações e falhas em DynamoDB ou CSV

Estrutura do Código
cp: Classe de configuração que contém os caminhos dos buckets e outras configurações.
zf: Classe com funções genéricas para operações de data, hash, e AWS S3/DynamoDB.
main(): Função principal que inicia o processo de transferência.

Manutenção
Consulte os registros de log no DynamoDB ou arquivos CSV para monitorar as operações e resolver quaisquer problemas.

Personalização
O script pode ser personalizado para se adequar a diferentes infraestruturas AWS e requisitos específicos de negócios.