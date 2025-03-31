# Projeto Criptomoedas

Este projeto tem como objetivo realizar requisições à API de criptomoedas, processar os dados recebidos, e armazená-los em um banco de dados PostgreSQL. O banco de dados é gerenciado via Docker.

## Estrutura do Projeto

- **cadastro.py**: Faz a requisição à API de cadastro de criptomoedas, obtém os dados e gera um DataFrame com as informações relevantes.
- **historicocriptos.py**: Faz a requisição à API para obter o histórico das criptomoedas, processa os dados e gera um DataFrame.
- **volume.py**: Realiza a requisição à API para obter o volume de transações das criptomoedas e cria um DataFrame com os dados.
- **database_manager.py**: Gerencia o banco de dados PostgreSQL, inserindo os dados obtidos das APIs nas tabelas correspondentes.

## Banco de Dados

O banco de dados é criado e gerido via Docker, utilizando uma instância do PostgreSQL. As credenciais necessárias para a API e o banco de dados são armazenadas em um arquivo `.env`.

### Arquivo `.env`

As variáveis de ambiente do projeto, como credenciais da API e do banco de dados, são armazenadas no arquivo `.env`. Certifique-se de configurar corretamente esse arquivo antes de executar o projeto.

### Arquivo `profiles.yml`

O arquivo `profiles.yml` contém as configurações necessárias para conectar o projeto ao banco de dados no Docker, configurando as credenciais adequadas.

## Como Rodar

1. Clone este repositório.
2. Certifique-se de ter o Docker instalado e em funcionamento.
3. Crie e configure o arquivo `.env` com as credenciais da API e do banco de dados.
4. Execute o banco de dados PostgreSQL via Docker.
5. Rode os scripts Python para fazer as requisições à API e processar os dados.

## Dependências

- Docker (para o banco de dados PostgreSQL)
- Python 3.x
- Bibliotecas Python:
  - pandas
  - requests
  - psycopg2 (para interação com o PostgreSQL)
  - os 


## Relatorio Power BI

Para acessar as informações gerados no banco de dados foi criado um relatorio simples no power bi , para acompanhamento de algumas métricas, como :

- Valor Maximos e Minimos atingidos pela Moeda em um periodo de data selecionado pelo usuário
- Valor médio da moeda para o periodo analisado
- Valor em $ disponivel no mercado para as moedas analisas
- variação em % nas ultimas 24 horas
- Volume das ultimas 24 horas.
- Quantidade de criptos em mercado


Tambem foi criado um frame por meio do Figma para implementar o visual do relatório

[**Clique aqui para acessar o relatório no Power BI**](https://app.powerbi.com/view?r=eyJrIjoiOTc0YjUyZGYtNGNmOC00NmI2LTkyMWUtYzllNGNiN2Q1YmY1IiwidCI6IjcyZWIyZTFhLTU1NzQtNDE5MC1iYmI5LTFhYzBhN2UzMGQ4ZiJ9)







## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

