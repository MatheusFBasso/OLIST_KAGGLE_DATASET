# OLIST_KAGGLE_DATASET

# 1. Dataset escolhido

Foi escolhido o dataset da Olist, se mostrando um excelente e completo dataset para uso.

Link: [Kaggle Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data)

---
# 2. Estrutura dos Dados

Felizmente a Olist informou o data schema, indicando quais os campo que se relacionam, podemos chamar de "PKs" e serão importantes durante a criação da silver para conseguimos já garantir valores únicos.
![DATA SCHEMA](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/assets/62318283/8abb6d1d-4ceb-4102-874e-1b866ce4aae3)

---
# 3. BRONZE

Como a Kaggla disponibiliza uma lib em Python é mais simples para pegarmos os dados atualizados, por isso foi criada uma classe no notebook [/bronze/DATUM_B_KAGGLE_ING](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/blob/main/bronze/DATUM_B_KAGLE_ING.ipynb), realizando as seguintes etapas:
- Pegar os segredos do Azure Key Vault relacionados ao acesso a API
- Estabelcer a API como variável
- Validar se o dataset informado existe
- Verificar se os caminhos informados existem e estão corretos
- Se já temos arquivos

Como fio criada uma classe as variáveis já estão definidas por padrão, logo somente é necessário chamar a classe ao final do notebook retornando `True` quando tivemos atualização e `False` quando não houveram alterações, onde:
- Quando as pastas estão vazias é realizado o download
- Quando a data informada pela API é superior ao registrado nos arquivos (indicando que houveram atualizações)
- Caso nenhuma dos casos acima seja verdade o código apenas irá retornar `False` uma vez que não temos dados mais recentes

---
# 4. Silver

Cada arquivos disponibilizado é convertido para seguindo um schema a fim de assegurar a estrutura assim como a opção `nullable` em campos que se relacionam com outras tabelas. Para isso foi estabelecida uma conexão com a Azure para o Unity Catalog ser salvo dentro da própria Azure no lugar do Databricks (a fim de testes).

Cada arquivo `.csv` foi criado de acordo com seu nome original onde dentro do código é assegurado que estamos lendo somente o arquivo necessário dentro da pasta da bronze, e em seguida criamos uma tabela utilizando SQL dentro do Databricks a sim de simplificar a lógica usada.

As tabelas foram criadas com `USING DELTA` e com `LOCATION` sendo apontado para a Azure (uma fonte 'externa'), como vantagem de criarmos as tabelas dentro do catálogo da `Datum` (assim chamado no projeto) podemos criar o database `silver` e as tabelas dentro dele, para um uso mais simplificado na camada gold. Não foi utilizado o `PARTITIONED BY` nessa etapa por não ter sido identificado a necessidade de uma consulta rápida e sim a gravação.

---
# 5. Gold

Com as tabelas já criadas na silver basta chamarmos elas usando ```spark.table('catalog.database.delta_table')``` assim simplificando as análises realizadas:

- ANALISE PRAZO ENTREGA: retorna duas tabelas ao final com um resumo de como estão os tempos de entrega, onde:
    - 1: Agrupa pelas siglas (dentro do código foi determinado um padrão de avaliação dos prazos) informando qual a que ocorre com mais frequência proporcional as demais
    - 2: Agrupa pelo estado e siglas, informando qual a proporcionalidade por estado e por sigla, facilitando entender como o estado em questão está com os prazos
- PARES DE PRODUTO: retorna duas tabelas uma informando com um modelo de ML do próprio Spark chamado `FPGrowth` onde foi utilizado o seguite artigo da própria Databricks como base para a sua utilização [Simplify Market Basket](https://www.databricks.com/blog/2018/09/18/simplify-market-basket-analysis-using-fp-growth-on-databricks.html) onde o foco foi alterado para o par de categoria de produtos. Por fim temos as seguintes tabelas:
    - 1: Tabela com o modelo de ML onde é previsto as categorias de produtos comprados juntos
    - 2: Tabela com o agrupamento do dados atuais para termos a mesma visão, contudo a partir dos dados e não do modelo em ML nesse caso
- PRODUTOS MAIS VENDIDOS: retorna 3 tabelas onde:
    - 1: Agrupamento do valor vendido pelo dia da semana
    - 2: Agrupamento do valor vendido por mês
    - 3: Agrupamento do valor vendido por estado

---
# 6. Orquestração

Por questão de tempo foi selecionado o Workflow do próprio Databricks para realizar a orquestração dos notebooks e processo, onde basicamente temos a camada bronze composta para API de extração dos csvs e após sua conclusão o ínicio do processamento de todos os arquivos para delta tables dentro unity catalog disponibilizando essas tabelas para consultas (caso necessário) e por fim o uso das tabelas na camada gold para análises iniciais com um agendamento semanal toda segunda conforme a imagem abaixo:

![image](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/assets/62318283/fa3215f1-b5b9-46e0-84de-671f9cba5c58)

![image](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/assets/62318283/e15741a5-2e12-4d66-a745-fc6628a54afb)

---
# 7. Visualização

Novamente foi selecionada a solução da Databricks para o problema, onde foi utilizado o Dashboards (muito similar o uso ao Metabase na questão de podermos gerar os dados para os gráficos através de queries), criando um para apresentar todas as tabelas criadas na camada gold conforme a as duas imagens abaixo:

![image](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/assets/62318283/59ede0bc-0d30-4dd5-a001-11b656b75095)

![image](https://github.com/MatheusFBasso/OLIST_KAGGLE_DATASET/assets/62318283/ec7c3ae6-0365-4cdf-b908-967238219d0b)

---
# 8. Considerações

Tenho outro projeto onde estudo o uso de Docker Compose com Airflow e Pyspark para Delta Lake, noto que o uso do ambiente cloud é muito mais simples de usar (uma vez configurado), nesse projeto cria um Delta Lake local utilizando uma API e o Ariflow como orquestrador principal.

[DockerAirflowSparkDeltaTables](https://github.com/MatheusFBasso/DockerAirflowSparkDeltaTables)

No projeto foco em testar as funções relacionadas ao Delta Lake a partir do livro [Delta Lake: Up & Running](https://www.databricks.com/resources/ebook/delta-lake-running-oreilly), o projeto já funciona e atualiza os dados, mas ainda há melhorias a serem feitas.
