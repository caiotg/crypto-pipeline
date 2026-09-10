# crypto-pipeline

![CI](https://github.com/caiotg/crypto-pipeline/actions/workflows/ci.yml/badge.svg)

Pipeline ELT de mercado de criptomoedas: extrai dados da API pública da [CoinGecko](https://www.coingecko.com/en/api), carrega em um data warehouse Postgres e transforma em camadas analíticas com dbt — tudo orquestrado pelo Airflow e rodando em containers Docker isolados.

Projeto de portfólio construído para demonstrar competências de engenharia de dados: orquestração, modelagem em camadas, testes automatizados, tratamento de falhas reais de API/rede/tipo de dado, e uma decisão de arquitetura para contornar um conflito real de dependências entre ferramentas.

## Arquitetura

```
CoinGecko API
      │
      ▼
┌─────────────────────────────────────────────────┐
│  Airflow (DAG: crypto_market_elt)               │
│                                                 │
│  extract_markets  →  load_raw  →  run_dbt       │
│  (Python/requests)  (pandas/    (DockerOperator │
│                       psycopg2)   → container   │
│                                    dbt isolado) │
└─────────────────────────────────────────────────┘
      │                    │              │
      ▼                    ▼              ▼
  (dado em memória,   raw.raw_coins_market   dbt_caio.stg_coins_market (view)
   via XCom)          (Postgres, schema raw)  dbt_caio.mart_market_overview (table)
```

**Por que ELT, não ETL:** a transformação (camada dbt) acontece **depois** da carga no Postgres, não antes, em Python. Isso aproveita o poder de processamento do banco para operações analíticas (window functions, agregações) em vez de fazer isso em memória com pandas — mais escalável conforme o volume de dados cresce, e separa claramente "mover dado" (Airflow) de "transformar dado" (dbt).

## Stack

| Camada | Tecnologia | Por quê |
|---|---|---|
| Extração | Python (`requests`) | Cliente HTTP simples, com retry/backoff para rate limit |
| Orquestração | Apache Airflow 2.9 (TaskFlow API) | Agendamento, dependências entre tasks, observabilidade de execução |
| Armazenamento | PostgreSQL 15 (dois bancos separados) | Um para metadados do Airflow, outro para dados analíticos — desacopla o "estado do orquestrador" do "dado de negócio" |
| Transformação | dbt (`dbt-postgres`) | Modelagem em camadas, testes declarativos, documentação e lineage automáticos |
| Containerização | Docker + Docker Compose | Reprodutibilidade; cada serviço isolado |

## Decisões de arquitetura e por que foram tomadas

### 1. Dois bancos Postgres separados, não um só

`postgres-airflow` guarda apenas metadados de execução (histórico de DAGs, conexões, usuários). `postgres-analytics` (`crypto_db`) guarda os dados de negócio. Se o banco de metadados do Airflow precisar ser resetado, os dados coletados não são afetados — e vice-versa. Reflete uma separação comum em ambientes reais entre "banco de controle de uma ferramenta" e "banco analítico".

### 2. Dockerfile customizado em vez da imagem oficial do Airflow pura

As dependências (`pandas`, `apache-airflow-providers-postgres`, etc.) são instaladas **no build da imagem**, não em runtime a cada subida do container. Isso garante que todos os componentes do Airflow (webserver, scheduler, init) sobem já com o ambiente Python idêntico e pronto — sem depender de um passo manual de `pip install` a cada `docker compose up`.

### 3. `secret_key` fixa e compartilhada entre componentes

Por padrão, cada componente do Airflow (webserver, scheduler) gera uma chave aleatória própria ao subir. Como são containers diferentes, essas chaves não coincidem, e a comunicação interna entre webserver e scheduler (usada para buscar logs de execução) falha com erro 403. Definir `AIRFLOW__WEBSERVER__SECRET_KEY` fixa no `docker-compose.yml` resolve isso — necessário em qualquer setup com múltiplos componentes Airflow reais, não em modo "standalone" de tutorial.

### 4. Segredos fora do código e fora do Git

- API key da CoinGecko: variável de ambiente via `.env`, lida com `python-dotenv`, nunca hardcoded.
- Credenciais do Postgres usadas pelo Airflow: gerenciadas via *Connection* cadastrada na UI (`analytics_postgres`), não em string de conexão no código do DAG.
- Credenciais usadas pelo dbt: `profiles.yml` fica fora da pasta do projeto por padrão (convenção do próprio dbt); para a integração com Docker, foi criado um `profiles.yml` alternativo apontando para o host da rede interna Docker, mantido separado do perfil usado em desenvolvimento local.

### 5. Deduplicação por hora na camada de staging

O client da CoinGecko é chamado a cada execução do DAG (schedule de 6 em 6 horas). Execuções de teste ou reprocessamento podem gerar múltiplas capturas dentro da mesma janela horária. O model `stg_coins_market` usa uma window function (`ROW_NUMBER() OVER (PARTITION BY id, DATE_TRUNC('hour', ingested_at) ORDER BY ingested_at DESC)`) para manter apenas o registro mais recente de cada moeda por hora, evitando que ruído operacional (retries, testes manuais) infle indevidamente o histórico.

### 6. `catchup=False` no DAG

O Airflow, por padrão, tenta reprocessar retroativamente todas as janelas de schedule entre o `start_date` e o momento atual. Como este pipeline captura o **estado atual** do mercado (não dados históricos particionados por data), rodar retroativamente não faz sentido: não existe "preço de bitcoin de uma data passada" a ser recuperado chamando a API hoje. `catchup=False` evita processamento inútil e risco de estourar o rate limit da API.

### 7. dbt rodando em container separado (`DockerOperator`), não instalado junto com o Airflow

**Problema encontrado:** instalar `dbt-postgres` no mesmo `requirements.txt` do Airflow causa conflito de resolução de dependências (`pip` não consegue encontrar uma combinação de versões compatível entre `apache-airflow` e `dbt-core`, que competem por versões diferentes de bibliotecas como `click` e `jinja2`). Esse é um conflito conhecido na comunidade — a prática recomendada é nunca misturar os dois ambientes Python.

**Solução adotada:** uma segunda imagem Docker (`Dockerfile.dbt`), enxuta, contendo só Python + `dbt-postgres`, sem nenhuma dependência do Airflow. O Airflow não executa o dbt diretamente — uma task `DockerOperator` pede ao Docker Engine do host para subir um container efêmero a partir dessa imagem, aguarda a execução do `dbt run`, e captura o resultado.

**Trade-off assumido:** isso exige montar o socket do Docker do host (`/var/run/docker.sock`) dentro do container do Airflow, concedendo a ele permissão para criar/gerenciar outros containers do host — um nível de acesso amplo, aceitável em ambiente local de estudo, mas que em produção seria mitigado com um grupo Docker dedicado e GID mapeado corretamente (em vez de rodar o container como root, como feito aqui por simplicidade).

**Alternativas consideradas e descartadas:**
- **Cosmos** (Astronomer): abstrai esse problema, mas internamente também precisa de `VirtualenvExecutionMode` ou `DockerExecutionMode` — ou seja, não resolve o conflito sozinho, apenas adiciona uma camada sobre uma dessas soluções. Descartado por complexidade extra sem ganho imediato.
- **`PythonVirtualenvOperator`**: cria um venv dentro do próprio container do Airflow a cada execução — mais lento (sem cache configurado) e ainda expõe o container do Airflow a ter que gerenciar instalação do dbt internamente.

## Problemas reais encontrados e resolvidos

Documentados aqui porque fazem parte do processo real de construção do pipeline, não só o resultado final:

1. **Import `plugins.coingecko_client` falhando dentro do Airflow** — a pasta `plugins/` não é adicionada automaticamente ao `PYTHONPATH` do Airflow. Resolvido definindo `PYTHONPATH: /opt/airflow` explicitamente.
2. **`psycopg2.ProgrammingError: can't adapt type 'dict'`** — o campo `roi` retornado pela CoinGecko vem como `None` para algumas moedas e como um dicionário aninhado para outras (schema inconsistente entre registros, comum em APIs REST reais). Resolvido serializando o campo para JSON string antes da carga.
3. **Execuções represadas no schedule após o container ficar parado por dias** — o Airflow, ao perceber que uma janela de schedule já passou, dispara a execução pendente mais recente assim que volta a rodar, mesmo com `catchup=False`. Comportamento esperado, mas que evidencia uma limitação real: um schedule de Airflow só se comporta como "intervalo fixo do relógio" se o serviço estiver rodando continuamente — inviável em uma máquina pessoal ligada e desligada, e um dos motivos pelos quais pipelines de produção rodam em infraestrutura sempre ativa.
4. **Conflito de dependências `dbt-core` × `apache-airflow`** — detalhado na seção de arquitetura acima.

## Como rodar

### Pré-requisitos
- Docker Desktop
- Conta CoinGecko Demo (API key gratuita) em [coingecko.com/api/pricing](https://www.coingecko.com/en/api/pricing)

### Setup

```bash
# 1. Variáveis de ambiente
cp .env.example .env
# preencher COINGECKO_API_KEY no .env

# 2. Build das imagens
docker build -f Dockerfile.dbt -t crypto-dbt:latest .
docker compose up -d --build

# 3. Inicializar o Airflow (primeira vez)
docker compose up airflow-init

# 4. Criar o schema no banco analítico (via DBeaver/psql, conectado em localhost:5433)
CREATE SCHEMA IF NOT EXISTS raw;

# 5. Cadastrar a connection do Airflow
# UI (localhost:8080) → Admin → Connections → analytics_postgres
# host: postgres-analytics | port: 5432 | schema: crypto_db | login/senha: analytics
```

Acesse `http://localhost:8080` (usuário/senha: `admin`/`admin`), ative o DAG `crypto_market_elt` e dispare manualmente ou aguarde o schedule (a cada 6 horas).

### Rodando o dbt isoladamente (fora do Airflow, para desenvolvimento)

```bash
cd crypto_pipeline_dbt
dbt run
dbt test
dbt docs generate && dbt docs serve
```

## Estrutura do repositório

```
crypto-pipeline/
├── docker-compose.yml
├── Dockerfile              # imagem do Airflow
├── Dockerfile.dbt          # imagem isolada do dbt
├── requirements.txt
├── dags/
│   └── crypto_market_elt.py
├── plugins/
│   └── coingecko_client.py
├── tests/
│   └── test_coingecko_client.py
└── crypto_pipeline_dbt/
    ├── models/
    │   ├── staging/
    │   │   ├── sources.yml
    │   │   └── stg_coins_market.sql
    │   └── marts/
    │       ├── schema.yml
    │       └── mart_market_overview.sql
    ├── tests/
    │   ├── assert_price_is_positive.sql
    │   └── assert_mart_is_recent.sql
    └── profiles_docker/
        └── profiles.yml
```

## Próximos passos (fora do escopo atual)

- CI (GitHub Actions) rodando `pytest` a cada push
- Testes de integração do dbt em pipeline de CI (exigiria um banco de teste efêmero)
- Dashboard de consumo (Streamlit ou Metabase) sobre `mart_market_overview`
