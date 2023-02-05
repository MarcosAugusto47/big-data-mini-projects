# Question 1 ----
## A
if (!require("pacman")) install.packages("pacman")
p_load(data.table,
       dplyr,
       geobr,
       sparklyr,
       purrr,
       stringr,
       lubridate, 
       arrow,
       read.dbc,
       microbenchmark)

dir.create('datasus')

years = 1996:2020 %>% as.character()
states = read_state() %>%
  as_tibble() %>%
  select(abbrev_state) %>%
  as.vector() %>% 
  .[[1]]

origin = 'ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/'

for (i in seq_along(states)){
  
  for (j in seq_along(years)){
    download_link = paste0(origin, "1996_/Dados/DNRES/DN",
                           states[i], years[j], '.dbc')
    
    download.file(url=download_link,
                  destfile=paste0('datasus/', states[i], years[j], '.dbc'),
                  mode = 'wb')
    
  }
  
}

## B
dbc_files = list.files(path='datasus/',
                       pattern='.dbc',
                       full.names=TRUE)
dbc_ESGOMS_files = list.files(path='datasus/',
                              pattern="GO|MS|ES",
                              full.names=FALSE)

dbc_to_csv = function(file, currfolder, destfolder, cols){
  
  path = paste0(currfolder, file)
  
  if (missing(cols)){
    df = read.dbc(path)
  }
  
  else {
    df = read.dbc(path) %>% select(all_of(cols))
  }
  
  df = df %>% mutate_all(as.character)
  csv_path = paste0(destfolder, gsub('.dbc', '.csv', file))
  write.csv(df, file = csv_path)
}

dir.create('ESGOMS_csv/')
walk(.x=dbc_ESGOMS_files,
     .y=dbc_to_csv,
     currfolder='datasus/',
     destfolder='ESGOMS_csv/')

dbc_to_parquet = function(file, currfolder, destfolder, cols){
  
  path = paste0(currfolder, file)
  
  if (missing(cols)){
    df = read.dbc(path)
  }
  
  else {
    df = read.dbc(path) %>% select(all_of(cols))
  }
  
  df = df %>% mutate_all(as.character)
  parquet_path = paste0(destfolder, gsub('.dbc', '.parquet', file))
  write_parquet(df, sink = parquet_path)
}
dir.create('ESGOMS_parquet')
walk(.x=dbc_ESGOMS_files,
     .y=dbc_to_parquet,
     currfolder='datasus/',
     destfolder='ESGOMS_parquet/')

parquet_ESGOMS_files = list.files(path='ESGOMS_parquet/', full.names=TRUE)
csv_ESGOMS_files = list.files(path='ESGOMS_csv/', full.names=TRUE)

sum(sapply(parquet_ESGOMS_files, file.size))/10**6
sum(sapply(csv_ESGOMS_files, file.size))/10**6

## C
cols = read.dbc('datasus/ES1996.dbc') %>%
  select(-c('contador', 'CODOCUPMAE')) %>% 
  colnames()

walk(.x=dbc_ESGOMS_files,
     .y=dbc_to_csv,
     currfolder='datasus/',
     destfolder='ESGOMS_csv/',
     cols=cols)
walk(.x=dbc_ESGOMS_files,
     .y=dbc_to_parquet,
     currfolder='datasus/',
     destfolder='ESGOMS_parquet/',
     cols=cols)

config = spark_config()
config$spark.executor.cores = 4
config$spark.executor.memory = "8G"
sc = spark_connect(master="local", config=config)
spark_version(sc)

microbenchmark(
  parquet_option = spark_read_parquet(sc=sc,
                                      path='ESGOMS_parquet/',
                                      header=TRUE,
                                      memory=FALSE),
  csv_option = spark_read_csv(sc=sc,
                              name='sinasc',
                              path='ESGOMS_csv/',
                              header=TRUE,
                              delimiter=',',
                              charset='latin1',
                              infer_schema=FALSE,
                              memory=FALSE),
  times=10)

# Question 2 ----
## A
dir.create('filtered_parquet')

dbc_all_files = list.files(path='datasus/')
walk(.x=dbc_all_files,
     .y=dbc_to_parquet,
     currfolder='datasus/',
     destfolder='filtered_parquet/',
     cols=cols)

spark_read_parquet(sc=sc,
                   name = 'sinasc',
                   path='filtered_parquet/',
                   header=TRUE,
                   memory=FALSE)

NUMERIC_COLUMNS = c('IDADEMAE','QTDFILVIVO','QTDFILMORT',
                    'PESO', 'PARTO', 'CONSULTAS')

sinasc = tbl(sc, "sinasc") %>% 
  mutate_at(NUMERIC_COLUMNS, as.double) %>% 
  mutate(SEXO = case_when(SEXO=='M' ~ '1',
                          SEXO=='F' ~ '2',
                          SEXO=='I' ~ '0',
                          SEXO=='9' ~ '0',
                          TRUE ~ SEXO),
         RACACOR = case_when(RACACOR=='9' ~ NA,
                             RACACOR=='0' ~ NA,
                             TRUE ~ RACACOR),
         PARTO = case_when(PARTO==1 ~ 0,
                           PARTO==2 ~ 1,
                           TRUE ~ NA),
         QTDFILVIVO = na_if(QTDFILVIVO, 99),
         QTDFILMORT = na_if(QTDFILMORT, 99),
         APGAR1 = na_if(APGAR1, 99),
         APGAR5 = na_if(APGAR5, 99),
         DTNASC = to_date(DTNASC, "ddMMyyyy"),
         DAYWEEK = date_format(DTNASC, "E"),
         PESO = na_if(PESO, 9999),
  ) %>% 
  filter(LOCNASC!='5', IDADEMAE>0, PESO>0, !is.na(PARTO), !is.na(CONSULTAS))

#sdf_register(df_sinasc, "sinasc_spark")
#tbl_cache(sc, "sinasc_spark")

# sinasc %>% count(LOCNASC)
# sinasc %>% 
#   mutate(IDADEMAE_flag = IDADEMAE==0) %>% 
#   count(IDADEMAE_flag)
# sinasc %>% count(PARTO)
# sinasc %>% count(ESTCIVMAE)
# sinasc %>% count(ESCMAE) # ESCMAE=7,8?
# sinasc %>% count(QTDFILVIVO)
# sinasc %>% count(QTDFILMORT)
# sinasc %>% count(GESTACAO) # GESTACAO=8?
# sinasc %>% count(GRAVIDEZ) # GRAVIDEZ=4?
# sinasc %>% count(PARTO)
# sinasc %>% count(CONSULTAS) # CONSULTAS=8?
# sinasc %>% count(SEXO) 
# sinasc %>% count(APGAR1) 
# sinasc %>% count(APGAR5) 
# sinasc %>% count(APGAR5) # TERMINATES R SESSION
# sinasc %>% count(RACACOR) 
# sdf_describe(sinasc, "PESO")

## B
## During prototyping, you might want to execute these transformations eagerly
## on a small subset of the data

# partition_sample = sample %>% 
#   sdf_random_split(training = 0.80, test = 0.20, seed=47)
# # register the resulting Spark SQL in Spark
# sdf_register(partition_sample$train, "train")
# sdf_register(partition_sample$test, "test")
# # loads the results into an Spark RDD in memory
# tbl_cache(sc, "train")
# tbl_cache(sc, "test")
# train = tbl(sc, 'train')

pipeline =  ml_pipeline(sc) %>%
  ft_imputer(input_col='IDADEMAE', output_col='IDADEMAE_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='PESO', output_col='PESO_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='QTDFILVIVO', output_col='QTDFILVIVO_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='QTDFILMORT', output_col='QTDFILMORT_IMPUTED',
             strategy='median')

## C
pipeline = pipeline %>% 
  ft_vector_assembler(
    input_col = c('IDADEMAE_IMPUTED', 'PESO_IMPUTED',
                  'QTDFILVIVO_IMPUTED', 'QTDFILMORT_IMPUTED'),
    output_col = "numerical_features"
  ) %>% 
  ft_standard_scaler(input_col="numerical_features",
                     output_col="numerical_features_scaled",
                     with_mean = TRUE)
  
## D
pipeline = pipeline %>% 
  ft_string_indexer(input_col = 'DAYWEEK', output_col='DAYWEEK_indexed') %>% 
  ft_one_hot_encoder(
    input_cols = c('DAYWEEK_indexed'),
    output_cols = c('DAYWEEK_encoded')
  ) %>% 
  ft_binarizer(input_col = "CONSULTAS",
               output_col = "CONSULTAS_binarized",
               threshold = 3.9) %>% 
  ft_vector_assembler(
    input_col = c('IDADEMAE_IMPUTED', 'PESO_IMPUTED',
                  'QTDFILVIVO_IMPUTED', 'QTDFILMORT_IMPUTED',
                  'DAYWEEK_encoded', 'CONSULTAS_binarized',
                  'numerical_features_scaled'),
    output_col = "final_features"
  )

## E
partition_sample = sinasc %>% 
  sdf_random_split(training=0.80, test=0.20, seed=47)

# register the resulting Spark SQL in Spark
sdf_register(partition_sample$train, "train")
sdf_register(partition_sample$test, "test")
tbl_cache(sc, "train")
tbl_cache(sc, "test")

pipeline = pipeline %>% 
  ml_logistic_regression(features_col="final_features", 
                         label_col="PARTO")

pipeline_model = ml_fit(pipeline, tbl(sc, 'train'))

ml_save(x=pipeline_model, path='sinasc_model', overwrite=FALSE) 
reloaded_model = ml_load(sc, "sinasc_model")

predictions = ml_transform(x=reloaded_model, dataset=tbl(sc, 'test'))
predictions %>% count(PARTO, prediction)

spark_disconnect(sc)

# complete pipeline just for illustration
pipeline =  ml_pipeline(sc) %>%
  ft_string_indexer(input_col = 'DAYWEEK', output_col='DAYWEEK_indexed') %>% 
  ft_one_hot_encoder(
    input_cols = c('DAYWEEK_indexed'),
    output_cols = c('DAYWEEK_encoded')
  ) %>% 
  ft_binarizer(input_col = "CONSULTAS",
               output_col = "CONSULTAS_binarized",
               threshold = 3.9) %>%
  ft_imputer(input_col='IDADEMAE', output_col='IDADEMAE_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='PESO', output_col='PESO_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='QTDFILVIVO', output_col='QTDFILVIVO_IMPUTED',
             strategy='median') %>% 
  ft_imputer(input_col='QTDFILMORT', output_col='QTDFILMORT_IMPUTED',
             strategy='median') %>%
  ft_vector_assembler(
    input_col = c('IDADEMAE_IMPUTED', 'PESO_IMPUTED',
                  'QTDFILVIVO_IMPUTED', 'QTDFILMORT_IMPUTED'),
    output_col = "numerical_features"
  ) %>% 
  ft_standard_scaler(input_col="numerical_features",
                     output_col="numerical_features_scaled",
                     with_mean = TRUE) %>%
  ft_vector_assembler(
    input_col = c('IDADEMAE_IMPUTED', 'PESO_IMPUTED',
                  'QTDFILVIVO_IMPUTED', 'QTDFILMORT_IMPUTED',
                  'DAYWEEK_encoded', 'CONSULTAS_binarized',
                  'numerical_features_scaled'),
    output_col = "final_features"
  ) %>%
  ml_logistic_regression(features_col="final_features", 
                         label_col="PARTO")