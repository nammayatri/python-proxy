# Setup:

```sh
python3 -m pip install --upgrade pip wheel
python3 -m venv gps-server 

source gps-server/bin/activate

brew install librdkafka
export C_INCLUDE_PATH=/opt/homebrew/Cellar/librdkafka/2.10.0/include
export LIBRARY_PATH=/opt/homebrew/Cellar/librdkafka/2.10.0/lib

python3 -m pip install -r requirements.txt 
```