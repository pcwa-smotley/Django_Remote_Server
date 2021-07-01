import requests
import pandas as pd

user = "pwca"
password = "p9071p"
session = requests.Session()
session.auth = (user, password)
hostname = "https://www.prt-inc.com/forecast/Private/CaisAhdThNp15GenApnd/forecast.htm"

response = requests.get(hostname, auth=(user, password))
df_prt = pd.read_html(response.text, header=0)
test = response
