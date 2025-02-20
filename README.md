### Installation

1. Get a free confluent account and API Key at [https://confluent.io](https://confluent.io)
2. Clone the repo
   ```sh
   git clone https://github.com/jw408/confluent_benchmarks.git
   ```
3. Create a virtual environment and install dependencies
   ```sh
   python3 -m venv .venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
       -or-
   pip install -r requirements.txt
   ```
4. Create a cluster and default topic in confluent web ui
   Under Home > Environments -> default -> (cluster name) -> Topics
     -> configuration
     -> Expert mode 
     mesage_timestamp_type = LogAppendTime 
     ^^ this sets the timestamp of when it is received by the broker, so we can measure round trip time  
5. Enter your secrets in .env
   ```sh
   cat > .env
   export CONFLUENT_API_KEY="XXXXXXXXXXXXXXXX"
   export CONFLUENT_API_SECRET="1XXXXXXXXXXXXXXXXXXXXXXXXXXXXXF"
   export CONFLUENT_CLUSTER_URL="pkc-XXX.confluent.cloud:9092"
   ^D

   ```
