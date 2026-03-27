# bookreads-elasticsearch
A local search engine project using Elasticsearch.

## Project Structure 

### Root Directory

- /data: dataset files
- /dev_env: development environment configs (includes elastic-start-local for local Elasticsearch/Kibana).
- /src: Python scripts and modules.
- README.md: Providing an overview and documentation.
- requirements.txt: Provides required python libraries.

## Quick Start

1. Clone the repo and navigate into it:
```bash
git clone git@github.com:johnpeterleo/bookreads-elasticsearch.git
cd bookreads-elasticsearch
```

#### a) Install Docker Desktop if you don’t have it:

Make sure Docker is running before proceeding.

- Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop)
- If using Windows, install [WSL](https://learn.microsoft.com/en-us/windows/wsl/install)

#### b) Install all requirements:
```bash
pip install -r requirements.txt
```

3. Start Elasticsearch and Kibana locally:
```bash
cd elastic-start-local
docker compose up -d # start Elasticsearch and Kibana
docker ps            # verify they are running
```

4. Access services:

- Elasticsearch login (API / backend) – for sending queries via REST API or clients:
You mostly interact with it via code, scripts, or curl. Visiting the URL in a browser just shows JSON status.
```bash
Elasticsearch: http://localhost:9200
Username: elastic
Password: YwGNRfez
```

- Kibana login (GUI / frontend) – for exploring data, dashboards, and visualizations:
```bash
Kibana: http://localhost:5601
Username: elastic
Password: YwGNRfez
```

5. Download bookreads kaggle data:
- Make a free Kaggle account
- Run the download script inside /src:
```bash
chmod +x download_data.sh
./download_data.sh
```
- Since some entries are incomplete, clean the dataset with:
```bash
python clean_data.py
```
This produces goodreads_clean.csv inside /data.

## Contact
John Christensen - johnchristensen@outlook.com


Alice Asmundsson - aliceasm@kth.se    


Kévin Bajul - kbajul@kth.se        


Sam Barati Bakhtiiari - sambb@kth.se  


