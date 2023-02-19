library(readxl)
library(dplyr)
library(tictoc)
library(furrr)
library(ggplot2)

# A ----
n=10**6
fifa_stats = read_excel("estatisticas-times.xlsx")
head(fifa_stats)

fifa_stats = fifa_stats %>% 
  mutate(GF_mean = GF/P, GS_mean = GS/P)
head(fifa_stats)

get_lambda = function(country_i, country_j, data){
  gf_mean_i = data %>% filter(country==country_i) %>% select(GF_mean) %>% pull()
  gs_mean_j = data %>% filter(country==country_j) %>% select(GS_mean) %>% pull()
  
  return((gf_mean_i + gs_mean_j)/2)
}

lambda_ij = get_lambda(country_i='Brazil', country_j='Serbia', data=fifa_stats)
lambda_ji = get_lambda(country_i='Serbia', country_j='Brazil', data=fifa_stats)

(prob_model = dpois(x=5, lambda=lambda_ij) * dpois(x=0, lambda=lambda_ji))
score = tibble(brasil = rpois(n=n, lambda=lambda_ij),
               serbia = rpois(n=n, lambda=lambda_ji))
(prob_empirical = score %>%
  count(outcome=(brasil==5 & serbia==0)) %>% 
  mutate(prob=n/sum(n)) %>% 
  filter(outcome==TRUE) %>% 
  pull())

# B ----
group_G = c("Brazil", "Serbia", "Switzerland", "Cameroon")
# create all six games combinations
games = combn(x=group_G, m=2) %>%
  t() %>%
  as_tibble() %>%
  setNames(c('home', 'guest'))

lambdas = games %>%
  left_join(fifa_stats, by=join_by(home==country)) %>% 
  left_join(fifa_stats, by=join_by(guest==country), suffix=c('_i', '_j')) %>% 
  mutate(lambda_ij = (GF_mean_i + GS_mean_j)/2,
         lambda_ji = (GF_mean_j + GS_mean_i)/2)

board = lambdas %>% select(home, guest, lambda_ij, lambda_ji)
n_games = 6

generate_simulation = function(i){
  
  # generate scored goals according to home team lambda
  board$GF_i = rpois(n=n_games, lambda=lambdas$lambda_ij)
  # generate scored goals according to visitor team lambda
  board$GF_j = rpois(n=n_games, lambda=lambdas$lambda_ji)
  
  # tag results
  board = board %>%
    mutate(result_i = case_when(GF_i > GF_j ~ 'VICTORY', 
                                GF_i < GF_j ~ 'LOSS',
                                TRUE ~ 'DRAW'),
           result_j = case_when(GF_j > GF_i ~ 'VICTORY', 
                                GF_j < GF_i ~ 'LOSS',
                                TRUE ~ 'DRAW'))
  
  # get outcomes according to home teams
  outcome_i = board %>%
    mutate(GS_i = GF_j) %>% select(home, GF_i, GS_i, result_i) 
  # get outcomes according to guest teams
  outcome_j = board %>%
    mutate(GS_j = GF_i) %>% select(guest, GF_j, GS_j, result_j)
  # bind everything to long format
  colnames(outcome_j) = colnames(outcome_i)
  outcome = outcome_i %>% bind_rows(outcome_j)
  
  # compute points by result
  outcome = outcome %>% 
    mutate(points = case_when(result_i == 'VICTORY' ~ 3,
                              result_i == 'DRAW' ~ 1,
                              TRUE ~ 0))
  
  # create classification board
  classification = outcome %>%
    group_by(home) %>%
    summarise(points=sum(points), 
              GF=sum(GF_i),
              GS=sum(GS_i),
              DIFF=GF-GS) %>%
    arrange(desc(points), desc(DIFF), desc(GF)) %>%
    mutate(pos = 1:n())
  
  # create final outcome for Brazil, tagging result for each rival and adding 
  # flag to check if Brazil classified
  output =
    # slice for Brazil results
    board %>%
    slice(1:3) %>%
    select(result_i) %>%
    # convert results to wide format
    t() %>%
    # flag if Brazil classified, i.e, final position 1 or 2
    cbind(classification %>%
            filter(home == 'Brazil') %>%
            select(pos) %>% pull() <= 2) %>%
    as.data.frame() %>% 
    setNames(c('serbia', 'switzerland', 'cameroon', 'is_classified')) %>% 
    mutate(is_classified = as.logical(is_classified))
    
    rownames(output) = i
    return(output)
}

# simulation example
generate_simulation(i=1)

tic()
plan(multisession, workers=4)
result = future_map(1:10000, ~ generate_simulation(.x))
toc()

final = do.call(bind_rows, result)
final %>%
  filter(serbia=='VICTORY') %>% select(is_classified) %>% pull() %>% mean()

final %>%
  filter(switzerland=='VICTORY') %>% select(is_classified) %>% pull() %>% mean()

final %>%
  filter(cameroon=='VICTORY') %>% select(is_classified) %>% pull() %>% mean()
