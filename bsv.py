import pandas as pd
import datetime 
from dateutil.tz import tzutc
import numpy as np
import requests
import time
import pickle

class query_control:
  counter = 0
  pager = 0
  def __init__(self):
    counter = 0
    pager = 0
  
  def add(self,my_pd):
    self.counter += 1
    self.check()
    self.stage_save(my_pd)
  
  def check(self):
    if self.counter >= 29:
      time.sleep(65)
      self.pager += 1
      print("this is the ", self.pager, " sleep")
      self.counter = 0

  def stage_save(self, my_pd):
    if self.pager % 10 == 0:
      filename = 'bsv_result' + str(self.pager) + '.csv'
      my_pd.to_csv(filename)

#read the attached file.
datetime_threshold = datetime.datetime(2017,7,1).astimezone(tzutc())

this_file = pd.read_csv('OUTPUTFILE-Table1.csv')

# split chains the btc address
btc_address_list = [row['wallet'] for index, row in this_file.iterrows() if row['coin'] == "BTC"]
btc_address_list = set(btc_address_list)
bsv_address_list = [row['wallet'] for index, row in this_file.iterrows() if row['coin'] == "BSV"]
bsv_address_list = set(btc_address_list)
bsp_address_list = [row['wallet'] for index, row in this_file.iterrows() if row['coin'] == "BSP"]
bsp_address_list = set(bsp_address_list)
true_bsp_address_list = list(bsv_address_list.union(bsp_address_list))
#pickle.dump(true_bsp_address_list, open('wallet_list.pkl', 'wb+'))
true_bsp_address_list = pickle.load(open('wallet_list.pkl', 'rb'))


controller = query_control()
container_bsv = pd.DataFrame(data=[], columns = ['wallet_address', 'block_id', 'transaction_id', 'index', 'transaction_hash', 'date', 'time', 'value', 'value_usd', 'recipient', 'type', 'script_hex', 'is_from_coinbase', 'is_spendable', 'is_spent', 'spending_block_id', 'spending_transaction_id', 'spending_index', 'spending_transaction_hash', 'spending_date', 'spending_time', 'spending_value_usd', 'spending_sequence', 'spending_signature_hex', 'lifespan', 'cdd'])
print("total wallet address", len(true_bsp_address_list))
address_counter = 0
for address in true_bsp_address_list[0:17]:
  address_counter += 1
  print("now working wtih address", address_counter)
  url = 'https://api.blockchair.com/bitcoin-sv/dashboards/address/' + address
  response = requests.get(url)
  controller.add(container_bsv)
  r = response.json()
  tx_list = list(set(r['data'][address]['transactions']))

  #bypass the query limit by aggregagte the query result
  total_tx = r['data'][address]['address']['transaction_count']
  print("this address has ", total_tx, ' tx')
  while True:
    num = len(tx_list)
    if num < total_tx:
      url = 'https://api.blockchair.com/bitcoin-sv/dashboards/address/' + address + '?offset=' + str(num-1)
      response = requests.get(url)
      controller.add(container_bsv)
      temp_r = response.json()
      temp_tx_list = list(set(temp_r['data'][address]['transactions']))
      tx_list = tx_list + temp_tx_list
    else:
      break
  #done tx list
  for tx in tx_list:
    tx_url = 'https://api.blockchair.com/bitcoin-sv/dashboards/transaction/' + tx
    tx_response = requests.get(tx_url)
    controller.add(container_bsv)
    tx_response = tx_response.json()

    in_out_list = tx_response['data'][tx]['inputs'] + tx_response['data'][tx]['outputs']
    
    for i in range(0, len(in_out_list)):
      tx_details = pd.DataFrame(in_out_list[i], index = range(i,i+1))
      tx_details['wallet_address'] = pd.Series(address, index=tx_details.index)
      container_bsv = container_bsv.append(tx_details)



