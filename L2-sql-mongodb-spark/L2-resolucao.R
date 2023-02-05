if (!require("pacman")) install.packages("pacman")
p_load(tidyverse,
       data.table,
       geobr,
       rmdformats,
       stringr, 
       vroom,
       mongolite,
       RSQLite,
       DBI,
       dbplyr,
       microbenchmark,
       sparklyr)

files = list.files(path='../L1-vroom-datatable-dtplyr/dados/',
                   full.names = TRUE)

COLS = c('estabelecimento_uf',
         'vacina_descricao_dose',
         'estabelecimento_municipio_codigo')
covid_subset = rbindlist(lapply(files, fread, select = COLS)) %>% as_tibble()

if (file.exists('covid_subset.csv')){
  warning("File exists")
} else {write.csv(covid_subset, 'covid_subset.csv')}

health_region = read_health_region() %>% as_tibble()

municipal_code = fread("../L1-vroom-datatable-dtplyr/Tabela_codigos.csv") %>%
  as_tibble()
colnames(municipal_code) = c('x',
                             'uf',
                             'municipio',
                             'cod_IBGE',
                             'cod_regiao_saude',
                             'nome_regiao_saude')

## ITEM A
mydb = dbConnect(RSQLite::SQLite(), "my-db.sqlite")

if (dbExistsTable(mydb, "covid_subset")) {
  warning("Table exists")
} else {dbWriteTable(conn=mydb, "covid_subset", covid_subset)}

if (dbExistsTable(mydb, "health_region")) {
  warning("Table exists")
} else {dbWriteTable(conn=mydb, "health_region", health_region)}

if (dbExistsTable(mydb, "municipal_code")) {
  warning("Table exists")
} else {dbWriteTable(conn=mydb, "municipal_code", municipal_code)}

## ITEM B
query = "SELECT nome_regiao_saude, n, classification, total
              FROM (
              SELECT *,
              ROW_NUMBER() OVER(PARTITION BY classification) AS index_median
              FROM (
              /* classificar em faixas de vacinação com base na mediana*/
              SELECT nome_regiao_saude, 
                     n,
                     total,
                     seq_num,
                     CASE WHEN seq_num <= (total + 1) / 2 THEN 'Baixa' ELSE 'Alta' END AS classification
              FROM (
              /*criar uma coluna com total de linhas e outra com o contador de 
              linhas para os os dados ordenados*/
              SELECT 
                *,
                COUNT(*) OVER() AS total, 
                ROW_NUMBER() OVER() AS seq_num
              FROM (
              /*juntar registros de vacinação com códigos de munícipio e agrupar
              por nome_regiao_saude*/
              SELECT 
                *,
                COUNT(*) AS n
               FROM
               covid_subset c
               LEFT JOIN municipal_code m
               ON c.estabelecimento_municipio_codigo=m.cod_IBGE
               GROUP BY nome_regiao_saude
               ORDER BY n ASC
              )))) WHERE index_median < 6 -- selecionar primeiros 5 registros"

sql_wrapper = function(){
  dbGetQuery(mydb, query)
}

sql_wrapper()

# ITEM C
url = 'mongodb://localhost:27017'
covid_subset_conn = mongo(collection = "covid_subset", db = "mydb", url=url)
covid_subset_conn$insert(covid_subset)

health_region_conn = mongo(collection = "health_region", db = "mydb", url=url)
health_region_conn$insert(health_region)

municipal_code_conn = mongo(collection = "municipal_code", db = "mydb", url=url)
municipal_code_conn$insert(municipal_code)

# col = mongo(db = "mydb", url = url)
# col$run('{"listCollections":1}')

covid_subset_conn$aggregate('[
                               {
                               "$lookup":
                                 {
                                 "from": "municipal_code",
                                 "localField": "estabelecimento_municipio_codigo",
                                 "foreignField": "cod_IBGE",
                                 "as": "code"
                                 }},
                               {"$out": "join_covid_municipal"}

                               
]')

covid_subset_join = covid_subset_conn$aggregate('[
                               {
                               "$lookup":
                                 {
                                 "from": "municipal_code",
                                 "localField": "estabelecimento_municipio_codigo",
                                 "foreignField": "cod_IBGE",
                                 "as": "code"
                                 }}
]')


join_covid_municipal_conn = mongo(collection = "join_covid_municipal",
                                  db = "mydb", url = url)

query = '[
          {"$group": {"_id": "$code.nome_regiao_saude", "count": {"$sum": 1}}}
         ]'
aggregation = join_covid_municipal_conn$aggregate(query,
                                                  options='{"allowDiskUse": true}')
aggregation = aggregation %>% 
  mutate(id = as.integer(`_id`))

# aggregation_conn = mongo(collection = "aggregation", db = "mydb", url = url)
# aggregation = aggregation %>% 
#   mutate(`_id` = as.character(`_id`))
# aggregation_conn$insert(aggregation)

# ITEM D
config = spark_config()
config$spark.executor.cores = 4
config$spark.executor.memory = "8G"
sc = spark_connect(master = "local", config = config)
spark_version(sc)

health_region = health_region %>% 
  as.data.frame() %>% 
  select(-"geom")

covid_subset_spark = spark_read_csv(sc=sc,
                                    name ='covid_subset',
                                    path = 'covid_subset.csv',
                                    header = TRUE,
                                    delimiter = ',',
                                    charset = 'latin1',
                                    infer_schema = TRUE)
health_region_spark = copy_to(sc, health_region, 'health_region',
                              overwrite = FALSE)
municipal_code_spark = copy_to(sc, municipal_code, 'municipal_code',
                               overwrite = FALSE)

sparklyr_wrapper = function(){
    covid_subset_spark %>%
    left_join(municipal_code_spark,
              by = c("estabelecimento_municipio_codigo" = "cod_IBGE")) %>% 
    group_by(nome_regiao_saude) %>% 
    summarise(n = n()) %>% 
    mutate(classification=if_else(n<=median(n), "Baixa", "Alta")) %>% 
    arrange(n) %>% 
    group_by(classification) %>% 
    filter(row_number() <= 5) %>% 
    collect()
}

sparklyr_wrapper()

# ITEM E
microbenchmark(
  sql_option = sql_wrapper(),
  sparklyr_option = sparklyr_wrapper(),
  times = 3)

spark_disconnect(sc) 


