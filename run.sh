## Setup
clear
sudo apt-get update
sudo apt-get upgrade -y
sudo apt-get install git python3 python3-pip python-is-python3 python3-venv -y
git clone http://github.com/abhayhk2001/sql-parsing
pip install virtualenv

cd sql-parsing
python -m venv env
source env/bin/activate
pip install -r requirements.txt
mkdir output extras
clear

# Run Program
cd sql-parsing/
sudo rm -rf output/* extras/*
source env/bin/activate
python parsing.py

copy .\results\results.csv ..\query-classification\results.csv /Y
cp ./results/results.csv ../query-classification/results.csv
