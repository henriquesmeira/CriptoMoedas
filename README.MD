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

## Relatório Power BI

Para acessar as informações geradas no banco de dados foi criado um relatório simples no Power BI para acompanhamento de algumas métricas, como:

- Valor máximo e mínimo atingidos pela moeda em um período de data selecionado pelo usuário
- Valor médio da moeda para o período analisado
- Valor em $ disponível no mercado para as moedas analisadas
- Variação em % nas últimas 24 horas
- Volume das últimas 24 horas.
- Quantidade de criptos em mercado

## Análises Geradas:

### 1. Gráficos Gerais
![Gráficos Gerais](Imagens/Indicadores%20Gerais.jpg)
Aqui podemos ver algumas métricas gerais, como o valor máximo e mínimo atingido pela moeda para o período selecionado no filtro de dados. Quando não existe nenhum filtro de moeda selecionado, o valor fica como 0.

### 2. Métricas Atuais
![Métricas Atuais](Imagens/Metricas%20Atuais%20das%20criptos.jpg)
Acompanhamento de valores atuais da moeda, como preço, volume de moedas no mercado, valor em $ presente no mercado e variação das últimas 24 horas.

### 3. Fechamento de Mercado
![Fechamento de Mercado](Imagens/valor%20de%20fechamento%20diario%20cripto.jpg)
Acompanhamento do valor de fechamento do mercado, possível ver os valores diários das moedas para os últimos 2 anos.

### 4. Comparação Bitcoin x Ethereum
![Comparação Bitcoin x Ethereum](Imagens/Preco%20Medio%20Diario.jpg)
Gráfico de comparação de dados de valores entre as moedas Bitcoin e Ethereum.

### 5. Relacionamento de Dados
![Relacionamento de Dados Feito no Power BI](Imagens/Relacionamento%20de%20dados.jpg)

Foi utilizada a estrutura de dados fornecida pela tabela do próprio banco de dados. Além disso, foi criada uma tabela Calendário dentro do modelo do Power BI para fazer análises relacionadas às datas, por meio da linguagem M.

#### **Código da Tabela Calendário (Linguagem M)**
```m
let
    // 1. Referenciar a tabela existente que contém as datas
    Fonte = #"public historicocriptos",  // Substitua pelo nome real da sua tabela
    ColunaData = time,  // Substitua pelo nome da coluna de data
    
    // 2. Extrair as datas mínima e máxima
    DataMinima = List.Min(Fonte[time]),
    DataMaxima = List.Max(Fonte[time]),
    
    // 3. Criar lista de datas do intervalo
    QuantidadeDias = Duration.Days(DataMaxima - DataMinima) + 1,
    ListaDatas = List.Dates(DataMinima, QuantidadeDias, #duration(1,0,0,0)),
    
    // 4. Converter para tabela e adicionar colunas
    TabelaCalendario = Table.FromList(ListaDatas, Splitter.SplitByNothing(), {"Data"}, null, ExtraValues.Error),
    
    // 5. Adicionar colunas derivadas
    CalendarioCompleto = Table.AddColumn(TabelaCalendario, "Ano", each Date.Year([Data]), Int64.Type),
    CalendarioCompleto1 = Table.AddColumn(CalendarioCompleto, "Mês", each Date.Month([Data]), Int64.Type),
    CalendarioCompleto2 = Table.AddColumn(CalendarioCompleto1, "Dia", each Date.Day([Data]), Int64.Type),
    CalendarioCompleto3 = Table.AddColumn(CalendarioCompleto2, "NomeMês", each Date.MonthName([Data]), type text),
    "#Tipo Alterado" = Table.TransformColumnTypes(CalendarioCompleto3,{{"Data", type date}})
in
    "#Tipo Alterado"
```

Também foi criado um frame por meio do Figma para implementar o visual do relatório.

[**Clique aqui para acessar o relatório no Power BI**](https://app.powerbi.com/view?r=eyJrIjoiOTc0YjUyZGYtNGNmOC00NmI2LTkyMWUtYzllNGNiN2Q1YmY1IiwidCI6IjcyZWIyZTFhLTU1NzQtNDE5MC1iYmI5LTFhYzBhN2UzMGQ4ZiJ9)

## Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

