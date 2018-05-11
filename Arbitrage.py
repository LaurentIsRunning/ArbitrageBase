import logging
from logging.handlers import RotatingFileHandler
import time
import asyncio
import ccxt.async as ccxt  # noqa: E402
from Tools import *
from GoogleSheetClient import *
from SmtpClient import *
from ExchangeManager import *

# Configure logging
logger = logging.getLogger("Arbitrage")
logger.setLevel(logging.INFO)
rotatingFileHandler = RotatingFileHandler("log.txt", maxBytes=500000, backupCount=50)
rotatingFileHandler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(rotatingFileHandler)
logger.addHandler(logging.StreamHandler())
logger.info("Starting...")

# Configuration de base
isSimulating = False
hasBalanceChanged = False
historyData= [] # Tableau qui contient l'historique des arbitrages potentiels qui seront envoyés sur googlesheet
operationProfitData = [] # Tableau qui contient les profits réalisés par cycle d'ordres envoyés

smtpClient = SmtpClient()

try:

  # Configuration
  googleClient = GoogleSheetClient()
  exchangeManager = ExchangeManager()
    
  exchanges = exchangeManager.CreateExchanges()
  logger.info('Loaded exchanges : ' + str([e.name for e in exchanges]))

  N = len(exchanges)

  # Récupération des balances
  exchangeManager.fetchBalances(exchanges)

  # En mode simulation,  définition de balances fictives
  if isSimulating:
    for exchange in exchanges:
      if not exchange.balance: exchange.balance = {}
      if 'free' not in exchange.balance: exchange.balance['free'] = {}
      exchange.balance['free']['EUR'] = 1000 / 1000
      exchange.balance['free']['USD'] = 1000 / 1000
      exchange.balance['free']['BTC'] = 0.15 / 1000
      exchange.balance['free']['ETH'] = 1.5 / 1000
      exchange.balance['free']['XMR'] = 5 / 1000
      exchange.balance['free']['BCH'] = 1.5 / 1000

  totalBalance = dict()
  for exchange in exchanges:
    if exchange.balance != None : 
      totalBalance = addDictionaries(totalBalance, exchange.balance['free'])
      
  initialTotalBalance = totalBalance.copy()
  previousTotalBalance = totalBalance.copy()
  logger.info('Initial total balance : ' + str(initialTotalBalance))
  googleClient.saveBalances(exchanges)
  
  while(True):
    minimum24VolumeRatio = 1000

    # Get all the tickers for specific assests
    exchangeManager.fetchTickers(exchanges, {'BTC', 'ETH', 'XMR', 'XLM', 'EOS', 'BCH', 'LTC'}, {'EUR'})
    # exchangeManager.fetchTickers(exchanges)

    # Compute volumes to order for the different types of assets
    # ----------------------------------------------------------
    volumes = {}
    volumes['BTC'] = 0.01
    volumes['EUR'] = 100
    volumes['USD'] = 125
    volumes['XMR'] = 0.3
    volumes['ETH'] = 0.15
    volumes['BCH'] = 0.1
    volumes['LTC'] = 0.5

    for exchange in exchanges:
      if exchange.tickers is None : continue
      for pair, ticker in exchange.tickers.items():
        if ('active' in exchange.markets[pair] and not exchange.markets[pair]['active'] 
            or ticker['ask'] == None or ticker['bid'] == None  
            or ticker['ask'] == 0 or ticker['bid'] == 0): 
            continue
        base, quote = pair.split('/')

        if base in volumes: continue

        if quote == "BTC":
          volumes[base] = volumes['BTC'] / ticker['ask']
        elif base == 'BTC':
          if quote in volumes: continue
          volumes[quote] = volumes['BTC'] * ticker['bid']
        
    # Compute the potential arbitrages
    # ------------------------------
    # We buy on exchange 'b' and we shell on exchange 's' an identical volume -> the amount of base asset will not change
    # However, 
    #  - on exchange 'b', we pay the lowest asked price :  ASKb * V 
    #  - and on exchange 's' we receive the highest bid price : BIDs * V
    # The profit is then equal to :  BIDs * V - ASKb * V.
    # In order to get a relative value in %, we will divide this profit by BIDs * V.
    arbitrages = []
    
    for b in range(N):
      
      if exchanges[b].tickers is None : continue
      
      for s in range(N):

        if b == s or exchanges[s].tickers is None : continue

        for pair in exchanges[b].tickers:
      
          if pair not in exchanges[s].tickers: continue

          base, quote = pair.split('/')

          buyTicker = exchanges[b].tickers[pair]
          sellTicker = exchanges[s].tickers[pair]

          # We check that the markets are actually active and that the volumes exchanged are sufficient
          if ('active' in exchanges[b].markets[pair] and not exchanges[b].markets[pair]['active'] 
              or 'active' in exchanges[s].markets[pair] and not exchanges[s].markets[pair]['active']
              or buyTicker['ask'] == None or buyTicker['bid'] == None or sellTicker['ask'] == None or sellTicker['bid'] == None
              or buyTicker['ask'] == 0 or buyTicker['bid'] == 0 or sellTicker['ask'] == 0 or sellTicker['bid'] == 0
              or buyTicker['baseVolume'] < minimum24VolumeRatio * volumes[base]
              or sellTicker['baseVolume'] < minimum24VolumeRatio * volumes[base]): 
            continue

          if sellTicker['bid'] is None or buyTicker['ask'] is None: continue

          if buyTicker['ask'] == 0 or sellTicker['bid'] == 0 : continue
          profit = 100 * (sellTicker['bid'] - buyTicker['ask'])/sellTicker['bid']
          
          arbitrage = {
            'profit' : profit, 
            'buy':{'exchange':exchanges[b], 'ticker' : buyTicker},
            'sell':{'exchange':exchanges[s], 'ticker' : sellTicker},
            'quote':quote,
            'base':base
            }

          baseVolume = volumes[base]
          quoteAmount = baseVolume * buyTicker['ask']
          
          # We check that the amounts of assets are sufficient to place the orders:
          isBaseFundingRequired = exchanges[s].balance is None or baseVolume is None or base not in exchanges[s].balance['free'] or exchanges[s].balance['free'][base] < baseVolume
          isQuoteFundingRequired = exchanges[b].balance is None or quoteAmount is None or quote not in exchanges[b].balance['free'] or exchanges[b].balance['free'][quote] < quoteAmount
          # If amounts of assets are not sufficient, could we make a transfert?
          isQuoteTransfertPossible = quote in totalBalance and quoteAmount is not None and quoteAmount < totalBalance[quote]
          isBaseTransfertPossible = base in totalBalance and baseVolume is not None and baseVolume < totalBalance[base]

          # If amounts of assets are sufficient, we keep the potential arbitrage
          if (not isBaseFundingRequired and not isQuoteFundingRequired): 
            arbitrages.append(arbitrage) 
          

    # Sorting to consider the best arbitrages first
    arbitrages.sort(key = lambda a:a['profit'], reverse=True)
          
    # Select arbitrages
    # -----------------
    usedExchanges = set()
    selectedArbitrages = []
    for arbitrage in arbitrages:
      # If the exchange is already used we continue
      if arbitrage['buy']['exchange'] in usedExchanges or arbitrage['sell']['exchange'] in usedExchanges: 
        continue

      # The arbitrage si selected if its profit is sufficient
      if exchangeManager.isArbitrageSufficient(arbitrage):
        selectedArbitrages.append(arbitrage)
        usedExchanges.add(arbitrage['buy']['exchange'])
        usedExchanges.add(arbitrage['sell']['exchange'])
        continue

    # Place the orders
    # -------------------
    if len(selectedArbitrages):
      for arbitrage in selectedArbitrages:

        assert arbitrage['buy']['ticker']['symbol'] == arbitrage['sell']['ticker']['symbol'], "Symbol must be equalt in sell and buy tickers !"

        base, quote = arbitrage['buy']['ticker']['symbol'].split('/')

        # Define the actual volume : the goal is to avoid little amount of money remaining
        if arbitrage['sell']['exchange'].balance['free'][base] < 2 * volumes[base] and arbitrage['buy']['exchange'].balance['free'][quote] > 1.05 * arbitrage['buy']['ticker']['ask'] * arbitrage['sell']['exchange'].balance['free'][base]:
          volume = arbitrage['sell']['exchange'].balance['free'][base]
        else:
          volume = volumes[base]

        historyData.append({'arbitrage' : arbitrage, 'volume' : volume, 'isSimulating' : isSimulating})

        if not isSimulating :
          exchangeManager.placeOrders(arbitrage['buy']['exchange'], arbitrage['sell']['exchange'], arbitrage['buy']['ticker']['symbol'], volume, arbitrage['buy']['ticker']['ask'], arbitrage['sell']['ticker']['bid'])
          # Query the new balance
          exchangeManager.fetchBalances(exchanges)
          hasBalanceChanged = True
        else:
          # Simulating the order
          exchangeManager.simulatePlaceOrder(arbitrage, volume)
          hasBalanceChanged = True
          
      totalBalance.clear()
      for exchange in exchanges:
        if exchange.balance != None : 
          totalBalance = addDictionaries(totalBalance, exchange.balance['free'])
        else: logger.info('No balance for exchange ' + exchange.name + '!')

      operationProfit = substractDictionaries(totalBalance, previousTotalBalance)  
      operationProfitData.append(operationProfit) # Save operation profits
      totalProfit = substractDictionaries(totalBalance, initialTotalBalance)

      logger.info('Operation profit : ' + str(operationProfit))
      logger.info('Total profit : ' + str(totalProfit))
      previousTotalBalance = totalBalance.copy()

      for asset, amount in totalProfit.items():
        if amount < -volumes[asset]/1000 : 
          tryMultipleTimes(googleClient.saveProfit, 5, {'operationProfitData' : operationProfitData, 'isSimulating' : isSimulating})
          tryMultipleTimes(googleClient.saveHistory, 5, {'historyData' : historyData})
          raise Exception("Negative profit : " + asset + " : " + str(amount))

    else:
      
      if exchangeManager.isWaitingRequired:
        logger.info('Waiting required: wait for 10 seconds and create exchanges... ')
        time.sleep(10)
        exchangeManager.isWaitingRequired = False
        exchanges = exchangeManager.CreateExchanges()
        exchangeManager.fetchBalances(exchanges)
        time.sleep(2)
      else:
        time.sleep(3)
      
      # If there is no arbitrage to do, we take the time to send reporting information to the googlesheet.
      if len(operationProfitData) > 0 :
        tryMultipleTimes(googleClient.saveProfit, 5, {'operationProfitData' : operationProfitData, 'isSimulating' : isSimulating})
        if not isSimulating : smtpClient.sendProfit(operationProfitData, totalProfit)
        operationProfitData = []

      if len(historyData):
        tryMultipleTimes(googleClient.saveHistory, 5, {'historyData' : historyData})
        historyData = []

      if hasBalanceChanged : 
        tryMultipleTimes(googleClient.saveBalances, 5, {'exchanges':exchanges})
        hasBalanceChanged = False

except Exception as e:
  logger.exception((type(e).__name__) + " : " + str(e))
  smtpClient.sendException(e)
