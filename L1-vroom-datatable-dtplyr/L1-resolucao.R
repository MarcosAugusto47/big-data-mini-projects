# Questão 1
## A
options(timeout=600)

library(pacman)
library(rvest)
library(stringr)
library(dplyr)

dir.create('dados')

ufs_regex = "AC|AL|AM|AP"
ufs = c("AC", "AL", "AM", "AP")
parts = 1:3 

links = read_html("https://opendatasus.saude.gov.br/dataset/covid-19-vacinacao/resource/5093679f-12c3-4d6b-b7bd-07694de54173?inner_span=True") %>%
        html_elements("li") %>% 
        html_elements("a") %>% 
        html_attr("href")

download_links = links[grepl(ufs_regex, links)]
paths = do.call(paste0, expand.grid(ufs, parts) %>% arrange(Var1, Var2))

for (i in seq_along(download_links)) {
  
  download.file(url = download_links[i],
                destfile = paste0('dados/', paths[i], '.csv'))
  
}

## B
p_load(vroom)
ac1 = vroom(file = "dados/AC1.csv", 
            locale = locale("br", encoding = "UTF-8"),
            num_threads = 3) %>%
      mutate_at(vars(matches('paciente|vacina.*_nome')), as.factor)
  
COLS = c('paciente_idade',
         'paciente_enumSexoBiologico',
         'paciente_racaCor_valor',
         'vacina_grupoAtendimento_nome',
         'vacina_nome',
         'vacina_dataAplicacao')

summary(ac1[, COLS])

## C
files = do.call(paste0, expand.grid('dados/', list.files('dados/')))
(dados_folder_size = sum(sapply(files, file.size)))
(ac1_memory_R = object.size(ac1))
(ac1_hd = 282951680)

## D
janssen = vroom(file = pipe('findstr -i "document JANSSEN" dados\\AC1.csv'),
                delim = ";",
                locale = locale("br", encoding = "UTF-8"))

teste = as.data.frame(janssen)

janssen_memory_R = object.size(janssen)
(ac1_memory_R - janssen_memory_R)

## E
janssen_all = vroom(file = pipe("findstr JANSSEN dados\\*.csv"),
                    delim = ";",
                    locale = locale("br", encoding = "UTF-8"))

# 2
## A
library(data.table)
library(geobr)
files = list.files(path='dados/', full.names = TRUE)
COLS = c('estabelecimento_uf', 'vacina_descricao_dose', 'estabelecimento_municipio_codigo')
covid_subset = rbindlist(lapply(files, fread, select = COLS))

health_region = read_health_region() %>% as.data.table()

municipal_code = fread("Tabela_codigos.csv")
colnames(municipal_code) = c('x', 'uf', 'municipio', 'cod_IBGE', 'cod_regiao_saude', 'nome_regiao_saude')

## B
data_table_wrapper = function(){
  covid_subset_add = merge(covid_subset, municipal_code,
                           by.x = 'estabelecimento_municipio_codigo',
                           by.y = 'cod_IBGE',
                           all.x = TRUE)
  
  agg_region_vaccinated = covid_subset_add[, .N, by = nome_regiao_saude]
  vaccination_median = median(agg_region_vaccinated$N)
  agg_region_vaccinated = agg_region_vaccinated[, classification := ifelse (N <= vaccination_median, "Baixa", "Alta")]
  agg_region_vaccinated = agg_region_vaccinated[order(N)]
  
  bottom_low = agg_region_vaccinated[classification == "Baixa"][1:5, ]
  bottom_high = agg_region_vaccinated[classification == "Alta"][1:5, ]
  
  return(list(bottom_low, bottom_high))
}

data_table_wrapper()

## C
library(dtplyr)
lazy_covid_subset = lazy_dt(covid_subset)

dtplyr_wrapper = function(x, y){
  lazy_agg_region_vaccinated = x %>%
                                left_join(y, by = c("estabelecimento_municipio_codigo" = "cod_IBGE")) %>% 
                                group_by(nome_regiao_saude) %>% 
                                summarise(n = n()) %>% 
                                mutate(classification = if_else(n <= median(n), "Baixa", "Alta")) %>% 
                                arrange(n) %>% 
                                as_tibble()
  bottom_low = lazy_agg_region_vaccinated %>% filter(classification=='Baixa') %>% head(n=5)
  bottom_high = lazy_agg_region_vaccinated %>% filter(classification=='Alta') %>% head(n=5)
  
  return(list(bottom_low, bottom_high))
  
  }

dtplyr_wrapper(lazy_covid_subset, municipal_code)

## D
library(microbenchmark)
tbl_covid_subset = covid_subset %>% as_tibble()
tbl_municipal_code = municipal_code %>% as_tibble()

microbenchmark(
  data_table = data_table_wrapper(),
  dtplyr = dtplyr_wrapper(lazy_covid_subset, municipal_code),
  dplyr = dtplyr_wrapper(tbl_covid_subset, tbl_municipal_code),
  times = 2)
