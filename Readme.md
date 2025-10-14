# Desafio Técnico: Análise e Engenharia de Dados

Este documento é o guia central para a solução dos três desafios técnicos. Ele detalha a abordagem, as ferramentas e as instruções para verificação de cada um.

Os arquivos de código e outros artefatos estão organizados em pastas para facilitar a avaliação:

- `/etl_yahoofinance/`: Contém o script Python para o pipeline de dados financeiros.
- `/proposta_automacao_cloud/`: Contém a descrição e o diagrama da arquitetura proposta.
- `/limpeza_dados/`: Contém o script para a limpeza e análise da planilha de vendas.
- `/evidencias/`: Contém as capturas de tela que comprovam a execução e os resultados dos desafios.

---

## Execução com Docker

Para garantir a execução do projeto em qualquer ambiente, a solução foi "containerizada" com Docker, eliminando a necessidade de instalar Python e bibliotecas manualmente.

**Pré-requisitos:**

- Docker Desktop instalado e em execução.

A estrutura de arquivos conta com **Dockerfile**, **.dockerignore** e **requirements.txt** na pasta raiz do projeto.

### 1. Construir a Imagem Docker

No terminal, navegue até a pasta raiz e execute o comando abaixo para construir a imagem. Isso precisa ser feito apenas uma vez.

```bash
docker build -t desafio-tecnico .
```

### 2. Executar os Scripts

Após a imagem ser construída, você pode rodar cada desafio com um comando simples.  
O parâmetro `-v` é usado para dar ao container acesso seguro a arquivos locais, sem expor credenciais ou dados sensíveis.

#### Desafio 1 (ETL Yahoo Finance)

Este comando precisa de acesso ao seu arquivo credentials.json, que deve estar na pasta etl_yahoofinance/.
A forma mais recomendada de executar é usando o comando abaixo diretamente no terminal do VS Code, a partir da pasta raiz do projeto. Ele usa $(pwd) para encontrar o caminho automaticamente.

```bash
docker run --rm -w /app/etl_yahoofinance -v "$(pwd)/etl_yahoofinance:/app/etl_yahoofinance" desafio-tecnico python etl_finance.py
```

#### Desafio 3 (Limpeza de Dados)

Este comando executa o script de limpeza e análise da planilha de vendas.

Instruções:

1. Garanta que os arquivos de dados necessários estejam dentro da pasta limpeza_dados/.

2. Abra seu terminal (como PowerShell, Bash, etc.) e navegue até a pasta raiz deste projeto.

3. Copie e cole o comando abaixo. Ele encontrará a pasta limpeza_dados automaticamente, sem precisar editar o caminho.

```bash
docker run --rm -w /app/limpeza_dados -v "$(pwd)/limpeza_dados:/app/limpeza_dados" desafio-tecnico python limpeza.py
```

---

## Desafio 1: Coclearleta de Dados - ETL

### Objetivo

Desenvolver um pipeline de ETL em Python para coletar, processar e carregar dados diários do Yahoo Finance para as 10 empresas de maior valor de mercado, salvando o resultado em uma planilha no Google Sheets.

### Implementação

O script `etl_yahoofinance/etl_finance.py` foi estruturado em funções modulares que representam cada etapa do processo.

- **Seleção Dinâmica**: `get_top_market_cap_tickers` identifica o TOP 10 baseado no marketCap.
- **Extração e Enriquecimento**: `extract_and_enrich_data` coleta histórico e metadados por ticker e consolida no DataFrame.
- **Transformação**: `transform_final_dataframe` limpa, padroniza e ajusta formatos.
- **Carga no Google Sheets**: `load_to_gsheets` usa Conta de Serviço para upload seguro.

### Execução Manual

**Pré-requisitos:** Python + bibliotecas

**Configuração do Ambiente (Recomendado)**

Para manter as dependências do projeto isoladas, é altamente recomendado criar um ambiente virtual.

1.  **Crie o ambiente virtual:**

    ```bash
    python -m venv .venv
    ```

2.  **Ative o ambiente:**

    - No Windows (PowerShell):
      ```powershell
      .venv\Scripts\Activate.ps1
      ```
    - No Linux ou macOS:
      ```bash
      source .venv/bin/activate
      ```

3.  **Instale as dependências:**
    Com o ambiente ativado, instale todas as bibliotecas necessárias a partir do arquivo `requirements.txt`.
    ```bash
    pip install -r requirements.txt
    ```

**Configuração das Credenciais Google**

1. Crie uma _Conta de Serviço_ no [Google Cloud Platform (GCP)](https://console.cloud.google.com/).
   - Conceda o papel _Editor_.
   - Ative as APIs _Google Drive API_ e _Google Sheets API_.
2. Gere o arquivo de chave no formato JSON (ex.: `credentials.json`).
3. Coloque o arquivo `credentials.json` dentro da pasta:

```bash
/etl_yahoofinance/
```

4. No **Google Sheets**, crie a planilha de destino.
5. Compartilhe essa planilha com o e-mail da conta de serviço (informado no campo `client_email` do `credentials.json`) com permissão de **Editor**.

### Executando o Script\*\*

No terminal, a partir da **pasta raiz do projeto**, execute:

```bash
cd etl_yahoofinance
python etl_finance.py
```

### Segurança

O arquivo `credentials.json` contém chaves privadas e é sensível. Em um projeto versionado com Git, ele seria incluído em um arquivo `.gitignore` para nunca ser exposto em repositórios públicos.

### Evidências

Pasta `/evidencias/`:

- `terminal_desafio1yahoo.png` — execução do script sem erros.
- `planilhayahoofinance.png` — resultado final no Google Sheets.

---

## Desafio 2: Proposta de Automação na Cloud

### Objetivo

Projetar solução na Google Cloud Platform (GCP) que rode o ETL a cada hora, de forma **segura, econômica e serverless**.

### Arquitetura

Pasta `/proposta_automacao_cloud/` contém diagrama.

**Serviços e Funções:**

- **Cloud Scheduler**: agenda a execução.
- **Cloud Functions**: executa o código do ETL.
- **Secret Manager**: armazena `credentials.json` de forma segura.
- **Cloud Storage (opcional)**: backup histórico dos dados.
- **Cloud Logging & Monitoring**: registro e alertas.

### Fluxo

1. Cloud Scheduler aciona Cloud Function.
2. Cloud Function busca credenciais no Secret Manager.
3. Executa pipeline: extrai, transforma, carrega e (opcionalmente) salva backup.
4. Logging registra sucesso ou falha.

### Benefícios

- **Custo-eficiência**: paga-se só pelo uso.
- **Segurança**: chaves isoladas no Secret Manager.
- **Zero manutenção**: GCP gerencia infraestrutura.
- **Escalabilidade**: ajusta-se à demanda.

---

## Desafio 3: Limpeza de Dados

### Objetivo

Limpar, enriquecer e validar uma planilha de vendas da empresa "ObjetosTeca", transformando dados brutos em dataset confiável.

### Estratégia

Script `limpeza_dados/limpeza.py`:

1. **Limpeza Inicial**: remove linhas "fantasma".
2. **Saneamento de SKUs**: remove duplicatas.
3. **Mapeamento Seguro**: usa RapidFuzz + `.map()` para corrigir nomes e SKUs sem duplicar linhas.
4. **Correção Hierárquica de Datas**:
   - Prioridade 1: coluna `Data`.
   - Prioridade 2: colunas `Ano` e `Mês`, corrigindo formatos e nomes.
5. **Validação Final**: rejeita registros inválidos para aba de auditoria.
6. **Saída**: gera Excel `ObjetosTeca_Limpo.xlsx` com abas **Dados_Limpos** e **Dados_Removidos**.

### Evidências

Pasta `/evidencias/`:

- `tabela_suja.png` — dados brutos.
- `tabela_limpa(normalizada).png` — dados limpos.
- `terminal_limpezadadospart1.png` até `terminal_limpezadadospart6.png` — execução passo a passo.

```text
Respostas e Insights

--- RELATÓRIO DE ANÁLISE ---

1. Top 5 Produtos por Faturamento (Receita) Mensal:

  Mês 1:
NomeProduto         Receita
    CELULAR R$ 4,255,239.87
      DISCO R$ 1,998,303.27
      XADREZ R$ 1,838,606.46
        BOTA   R$ 898,104.33
    LANTERNA   R$ 876,921.55

  Mês 2:
NomeProduto         Receita
    CELULAR R$ 3,904,372.94
      DISCO R$ 3,051,672.03
      XADREZ R$ 1,147,119.09
        BOTA   R$ 795,287.49
        DADO   R$ 735,020.32

  Mês 3:
NomeProduto         Receita
    CELULAR R$ 5,880,210.50
      DISCO R$ 3,906,156.23
      XADREZ R$ 1,816,413.61
        DADO   R$ 892,899.95
    LANTERNA   R$ 854,079.78

  Mês 4:
NomeProduto         Receita
    CELULAR R$ 5,558,001.90
      DISCO R$ 3,117,898.05
      XADREZ R$ 1,819,107.97
        BOTA   R$ 897,180.55
        DADO   R$ 888,274.55

  Mês 5:
NomeProduto         Receita
    CELULAR R$ 5,230,236.40
      DISCO R$ 3,744,377.48
      XADREZ R$ 1,310,457.16
        BOTA R$ 1,055,777.27
    MOCHILA    R$ 729,601.76

  Mês 6:
NomeProduto         Receita
    CELULAR R$ 8,059,199.30
      DISCO R$ 2,410,629.61
      XADREZ R$ 1,357,840.79
        BOTA   R$ 971,539.75
        DADO   R$ 952,336.25

  Mês 7:
NomeProduto          Receita
    CELULAR R$ 11,050,718.80
      DISCO  R$ 3,328,655.46
        BOTA  R$ 1,934,700.44
      XADREZ  R$ 1,869,779.97
      AGULHA   R$ 910,580.60

  Mês 8:
NomeProduto          Receita
    CELULAR R$ 10,051,296.30
      DISCO  R$ 3,462,975.74
      XADREZ  R$ 1,755,535.04
        BOTA  R$ 1,076,785.34
        DADO   R$ 853,278.20

  Mês 9:
NomeProduto         Receita
    CELULAR R$ 9,894,818.90
      DISCO R$ 3,717,626.52
      XADREZ R$ 2,221,463.00
        BOTA R$ 1,299,152.72
    MOCHILA    R$ 887,409.01

  Mês 10:
NomeProduto         Receita
    CELULAR R$ 6,888,717.20
      DISCO R$ 3,841,804.73
      XADREZ R$ 1,507,629.55
        BOTA R$ 1,269,196.65
    LANTERNA   R$ 607,781.93

  Mês 11:
NomeProduto          Receita
    CELULAR R$ 21,614,218.50
      DISCO  R$ 5,781,839.70
      XADREZ  R$ 3,377,881.39
        BOTA  R$ 2,731,213.04
    MOCHILA  R$ 1,298,648.23

  Mês 12:
NomeProduto         Receita
    CELULAR R$ 9,627,704.30
      DISCO R$ 2,569,166.13
        BOTA R$ 1,391,158.25
      XADREZ R$ 1,381,284.41
    MOCHILA R$ 1,194,878.68


2. Top 5 Produtos com Menos Cliques (que tiveram receita e cliques > 0):

  Mês 1:
NomeProduto  Cliques
      APITO     1319
    CACHIMBO    3169
      LIVRO    14878
      XÍCARA    17503
        COPO    19840

  Mês 2:
NomeProduto  Cliques
  TELEVISÃO      125
    CACHIMBO    1813
      AGULHA    2372
      APITO     6347
        COPO    6495

  Mês 3:
NomeProduto  Cliques
      APITO     2740
    CACHIMBO    4456
        COPO    7238
      AGULHA    7971
      XÍCARA   13105

  Mês 4:
NomeProduto  Cliques
  TELEVISÃO        9
    CACHIMBO     984
      CANETA    1971
      APITO     2671
        COPO    4699

  Mês 5:
NomeProduto  Cliques
  TELEVISÃO      453
      CANETA    1124
      BOLSA     1610
      APITO     2505
    CACHIMBO    3175

  Mês 6:
NomeProduto  Cliques
      APITO     3296
      AGULHA    9604
        COPO   11466
    CACHIMBO   13425
    TECLADO    21170

  Mês 7:
NomeProduto  Cliques
  TELEVISÃO       76
      APITO     3456
      XÍCARA    3646
    TECLADO     9663
      COPO    15579

  Mês 8:
NomeProduto  Cliques
  TELEVISÃO      551
      BOLSA     2004
      XÍCARA    2085
      APITO     2993
    TECLADO    15786

  Mês 9:
NomeProduto  Cliques
  TELEVISÃO      834
      XÍCARA    4052
      APITO     4336
    TECLADO     6090
      COPO    16850

  Mês 10:
NomeProduto  Cliques
      CANETA     103
      TÊNIS      375
  TELEVISÃO      982
      XÍCARA    2418
      APITO     4198

  Mês 11:
NomeProduto  Cliques
      CANETA     250
  TELEVISÃO      439
      XÍCARA    1829
      TÊNIS     4065
      APITO     5560

  Mês 12:
NomeProduto  Cliques
  TELEVISÃO       53
      XÍCARA    3349
      APITO     4575
        COPO    5132
    CACHIMBO    9659


3. Top 5 Produtos por Receita Média por Registro (Anual):
NomeProduto
CELULAR   R$ 279,492.42
DISCO     R$ 114,014.22
XADREZ     R$ 58,638.68
BOTA       R$ 41,858.41
DADO       R$ 25,883.96


4. Insights Adicionais:

Recuperação Total e Valor da Engenharia de Dados:
O principal insight do processo é o sucesso da própria engenharia de dados. O pipeline foi capaz de corrigir e validar 100% dos registros, transformando um dataset com falhas críticas em uma fonte de análise completa e confiável. Sem este tratamento, as conclusões de negócio seriam baseadas em dados parciais e imprecisos.

Diagnóstico de Problemas Sistêmicos Corrigidos:
O sucesso na recuperação só foi possível porque o script foi desenhado para tratar múltiplos padrões de erro encontrados na fonte, indicando a necessidade de melhorias nos sistemas de entrada de dados. Os principais problemas superados foram:

Nomes de Produtos: Inconsistências e erros de digitação foram corrigidos com a técnica de fuzzy matching.

Formatos de Data: Múltiplas formas de registro (ex: DD-MM-AAAA, nomes de meses, anos de 2 dígitos) foram unificadas através de uma lógica hierárquica.

Placeholders Inválidos: Dados como 'YY' e 9999 foram tratados e corrigidos programaticamente, aproveitando outras informações da mesma linha.

Ferramenta de Auditoria Contínua:
Embora nesta execução todos os dados tenham sido recuperados, a aba Dados_Removidos no arquivo de saída permanece como um componente essencial do pipeline. Ela garante que quaisquer novos tipos de erros de dados no futuro sejam automaticamente capturados, quantificados e reportados, tornando a solução robusta a longo prazo.
```

---

## Bônus: Ferramentas e Tecnologias Utilizadas

### Programação e Análise de Dados

**Python**: Linguagem central para todas as soluções de código, devido ao seu ecossistema robusto para dados.

**Pandas**: Biblioteca principal para manipulação, limpeza e estruturação de dados em DataFrames.

**NumPy**: Essencial para operações numéricas e de alta performance, sendo uma dependência fundamental do Pandas.

**Openpyxl**: Motor utilizado pelo Pandas para ler e escrever arquivos no formato Excel (.xlsx).

### Coleta e Conectividade (Desafio 1)

**Yfinance**: Biblioteca para a extração de dados financeiros do Yahoo Finance.

**Gspread & Google Auth OAuthlib**: Conjunto de bibliotecas para a autenticação segura e manipulação de planilhas no Google Sheets.

**Python-Dateutil**: Utilizada para cálculos de data robustos (como relativedelta) no pipeline de ETL.

**Requests**: Biblioteca para fazer requisições HTTP, geralmente utilizada como dependência por outras bibliotecas de API.#

### Limpeza de Dados (Desafio 3)

**RapidFuzz**: Biblioteca de fuzzy matching para comparar e corrigir strings com erros de digitação (nomes de produtos).

### Infraestrutura e Design

**Docker**: Plataforma de contêineres utilizada para empacotar a aplicação e garantir a reprodutibilidade do ambiente.

**Visual Studio Code**: Ambiente de desenvolvimento principal.

**Lucidchart**: Ferramenta utilizada para a criação do diagrama da arquitetura serverless na GCP.

---

## Feedback sobre o Desafio

Primeiramente, gostaria de parabenizar a equipe pelo excelente desafio proposto. O teste reflete de forma muito fiel os cenários e as complexidades encontradas no dia a dia de um engenheiro de dados, tornando a experiência extremamente relevante e prática.

Achei a experiência muito gratificante e divertida. A etapa de normalização dos dados, especialmente o tratamento das datas, foi particularmente desafiadora, mas superá-la trouxe grande satisfação. Além disso, o desafio me proporcionou uma excelente oportunidade de aprendizado. Fiquei especialmente animada por ter meu primeiro contato prático com a Google Cloud. Embora já possuísse experiência com a AWS, explorar o ecossistema da GCP, mesmo em um projeto de escopo controlado, foi muito enriquecedor. A chance de trabalhar com novas bibliotecas e solidificar minhas habilidades com Docker também contribuiu para um aprendizado valioso.

---

## Sugestão para Futuras Edições

Como um ponto de feedback construtivo, sugiro que futuras versões do desafio possam incorporar o uso de um sistema de versionamento de código. Por exemplo, solicitar que a solução seja entregue em um repositório privado no GitHub. Acredito que isso poderia agregar ainda mais valor ao processo de avaliação, pois permitiria observar a linha de raciocínio do candidato através da análise de seus commits, além de avaliar a qualidade da documentação no `README` e a organização do projeto como um todo.

Agradeço novamente pela oportunidade.
#   d e s a f i o s - e n g e n h a r i a - d a d o s  
 