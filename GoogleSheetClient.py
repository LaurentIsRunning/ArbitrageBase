import logging
import httplib2
import os
import time

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

logger = logging.getLogger("Arbitrage")

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

class GoogleSheetClient:
  def __init__(self):
      credentials = self._get_credentials('CoinGenerator', 'client_secret.json', 'https://www.googleapis.com/auth/spreadsheets')
      http = credentials.authorize(httplib2.Http())
      discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
      self.service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discoveryUrl)
      self.spreadsheetId = 'your spreasheet'

  def _get_credentials(self, applicationName:str, clientSecretFile:str, scopes:str):
      """Gets valid user credentials from storage.

      If nothing has been stored, or if the stored credentials are invalid,
      the OAuth2 flow is completed to obtain the new credentials.

      Returns:
          Credentials, the obtained credential.
      """
      home_dir = os.path.expanduser('~')
      credential_dir = os.path.join(home_dir, '.credentials')
      if not os.path.exists(credential_dir):
          os.makedirs(credential_dir)
      credential_path = os.path.join(credential_dir, 'sheets.googleapis.com-python-arbitrage.json')

      store = Storage(credential_path)
      credentials = store.get()
      if not credentials or credentials.invalid:
          flow = client.flow_from_clientsecrets(clientSecretFile, scopes)
          flow.user_agent = applicationName
          if flags:
              credentials = tools.run_flow(flow, store, flags)
          else: # Needed only for compatibility with Python 2.6
              credentials = tools.run(flow, store)
          print('Storing credentials to ' + credential_path)
      return credentials
  
  def saveBalances(self, exchanges):
    balanceData = []
    for exchange in exchanges:
      if exchange.balance != None : 
        balanceData.extend([[exchange.name, asset, exchange.balance['free'][asset]] for asset in exchange.balance['free'] if exchange.balance['free'][asset] > 0])
    
    self.service.spreadsheets().values().clear(spreadsheetId=self.spreadsheetId, range='Balances!A2:C', body={}).execute()
    self.service.spreadsheets().values().update(spreadsheetId=self.spreadsheetId, range='Balances!A2:C', valueInputOption = 'RAW', body={'values' : balanceData }).execute()


  def saveProfit(self, operationProfitData, isSimulating):
    profitData = []
    now = time.strftime('%m/%d/%y %H:%M:%S')
    for operationProfit in operationProfitData:
      for asset in operationProfit:
        profitData.append([now, asset, operationProfit[asset], isSimulating])
    self.service.spreadsheets().values().append(spreadsheetId=self.spreadsheetId, range='Profits!A2:D', valueInputOption = 'USER_ENTERED', body={'values' : profitData }).execute()


  def saveHistory(self, historyData):
    now = time.strftime('%m/%d/%y %H:%M:%S')
    historyData = [
      [now, 
       "%.1f" % data['arbitrage']['profit'], 
       data['arbitrage']['buy']['ticker']['symbol'], 
       data['arbitrage']['buy']['exchange'].name, 
       data['arbitrage']['buy']['ticker']['ask'], 
       data['arbitrage']['sell']['exchange'].name, 
       data['arbitrage']['sell']['ticker']['bid'], 
       data['volume'], 
       data['isSimulating']] 
      for data in historyData]
    self.service.spreadsheets().values().append(spreadsheetId=self.spreadsheetId, range='History!A2:I', valueInputOption = 'USER_ENTERED', body={'values' : historyData }).execute()
