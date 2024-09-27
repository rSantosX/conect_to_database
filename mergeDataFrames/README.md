# Merge DataFrames

Nesse exemplo tenho o objetivo de juntar o resultado de dois SELECT, e retornar um determinado conjunto de colunas.

## Banco de dados
Estarei usando um banco de dados PostgreSQL


## Instalação de Libs

Use o gerenciador de pacotes [pip](https://pip.pypa.io/en/stable/), para realizar as seguintes instalações:

- pandas
- pyarrow
- psycopg2

```bash
pip install psycopg2 pyarrow pandas
```

## Como fazer o Merge

Podemos usar a função *pandas.merge()* para combinar os DataFrames. Após a mesclagem, podemos filtrar ou selecionar facilmente as colunas desejadas.


## Visão geral do cenário

Suponha que você tenha duas tabelas ou consultas, onde cada uma retorna um customer_id, mas com nomes de coluna diferentes para outros campos:

1- Tabela cliente (SELECT 1)

- Contem as colunas: ```id_cliente```(ID comum), ```documento```, ```nome```

2- Tabela vendas (SELECT 2)

- Contem as colunas: ```vendas_id```,```cliente_id```(representa id_cliente), ```produto_id```, ```valor```

