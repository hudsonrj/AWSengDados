README para Script de Automação AWS S3

Descrição Geral
Este script Python é utilizado para gerenciar o processo de transferência e processamento de arquivos entre diferentes buckets do AWS S3, especificamente para arquivos ZIP relacionados a dados de companhias aéreas. Ele verifica a integridade dos arquivos, compara hashes para evitar duplicatas, e move arquivos entre buckets de acordo com o resultado dessas verificações.

Configurações (cp)
A classe cp contém as configurações de caminhos dos buckets do S3, informações de acesso ao banco de dados DynamoDB, e configurações para envio de e-mails.

Funções Genéricas (zf)
A classe zf oferece funções para obtenção de data e hora atual, geração de números aleatórios, cálculo de hash SHA-512 de dados, conexão com o DynamoDB, consultas e inserções no banco de dados, descompactação de arquivos ZIP e outras utilidades.

Uso
Assegure-se de que todas as dependências estejam instaladas e que as credenciais da AWS estão configuradas corretamente.
Personalize as variáveis de configuração na classe cp conforme necessário.
Execute o script via terminal ou IDE Python com o comando:
bash
Copy code
python script.py

Funcionalidades
Verificação de arquivos: Avalia a existência e integridade dos arquivos ZIP.
Processamento de dados: Descompacta arquivos válidos e os move para o bucket de processamento.
Controle de integridade: Compara hashes para evitar processamento de arquivos duplicados.
Registro de eventos: Armazena logs de operações e erros no DynamoDB ou em arquivos CSV.

Manutenção e Log
Consulte o DynamoDB ou os arquivos CSV para acompanhar o processo e identificar quaisquer erros ou duplicações.

Personalização
Ajuste as classes e funções para atender aos requisitos específicos de sua infraestrutura AWS e fluxo de trabalho.

Contribuições
Contribuições são bem-vindas. Para contribuir, faça um fork do repositório, adicione suas melhorias e abra um pull request.

Suporte
Em caso de dúvidas ou problemas, abra um issue no repositório do GitHub ou contate o administrador do sistema.
