import logging
import time
import asyncio
import ccxt
import ccxt.async as ccxt  # noqa: E402
from Tools import *

logger = logging.getLogger("Arbitrage")

class ArbitrageException(Exception):
    """Base class for exceptions in this module."""
    pass


class ExchangeManager(object):
  """Wrapper around ccxt library"""

  def __init__(self, **kwargs):
    self.messageCounter = 0
    self.isWaitingRequired = False

  async def tryMultipleTimesAsync(self, exchange, func, maxRetries = 5, params:dict = None):
      
    description = exchange.name

    result = None

    #print('Start ', func.__name__, ' ', description)

    for numRetries in range(0, maxRetries):
      try:  
        self.messageCounter += 1
            
        if params is None : result = await func()
        else : result = await func(**params)
        break
      except ccxt.DDoSProtection as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
        self.isWaitingRequired = True
        break # If it's a DDoS error, it break directly.
      except ccxt.RequestTimeout as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__))
        self.isWaitingRequired = True
      except ccxt.AuthenticationError as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
      except ccxt.ExchangeNotAvailable as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
      except ccxt.ExchangeError as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
        await asyncio.sleep(0.5)
      except ccxt.NetworkError as e:
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
        self.isWaitingRequired = True
      except Exception as e: 
        logger.error(description + ' - ' + func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
    #print('End ', func.__name__, ' ', description)

    return result

  
  def loadMarkets(self, exchanges:list):
    async def loadMarket(exchange):
      await self.tryMultipleTimesAsync(exchange, exchange.load_markets)
    
    [asyncio.ensure_future(loadMarket(exchange)) for exchange in exchanges]
    pending = asyncio.Task.all_tasks()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(*pending))

  def fetchBalances(self, exchanges:list):
    async def fetchBalance(exchange):
      if exchange.apiKey != '': exchange.balance = await self.tryMultipleTimesAsync(exchange, exchange.fetch_balance)
      else : exchange.balance = None

    asyncio.get_event_loop().run_until_complete(asyncio.gather(*[asyncio.ensure_future(fetchBalance(exchange)) for exchange in exchanges]))


  def fetchTickers(self, exchanges:list, baseAssets = None, quoteAssets = None):
    def get_active_symbols(exchange, baseAssets = None, quoteAssets = None):
      return [symbol for symbol in exchange.symbols if is_active_symbol(exchange, symbol) and is_assets_available(symbol, baseAssets, quoteAssets)]

    def is_active_symbol(exchange, symbol):
      return ('.' not in symbol) and (('active' not in exchange.markets[symbol]) or (exchange.markets[symbol]['active']))

    def is_assets_available(symbol, baseAssets, quoteAssets):
      base, quote = symbol.split('/')
      return (baseAssets is None or base in baseAssets) and (quoteAssets is None or quote in quoteAssets)

    async def fetchTickersAllAtOnce(exchange):
      #logger.info('   - Fetching tickers all at once for ' + exchange.name)
      exchange.tickers = await self.tryMultipleTimesAsync(exchange, exchange.fetch_tickers)

    async def fetchTickersOneByOne(exchange):
      #logger.info('   - Fetching tickers one by one for ' + exchange.name)
      symbols_to_load = get_active_symbols(exchange, baseAssets, quoteAssets)
   
      input_coroutines = [asyncio.ensure_future(self.tryMultipleTimesAsync(exchange, exchange.fetch_ticker, 5, {'symbol':symbol})) for symbol in symbols_to_load]
      tickers = await asyncio.gather(*input_coroutines)
      exchange.tickers = {t['symbol'] : t for t in tickers if isinstance(t, dict)}

    start = time.time()
    tasks = [asyncio.ensure_future(fetchTickersAllAtOnce(exchange)) for exchange in exchanges if exchange.has['fetchTickers']] + [asyncio.ensure_future(fetchTickersOneByOne(exchange)) for exchange in exchanges if not exchange.has['fetchTickers']]
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))

    print(self.messageCounter, ' - Fetch tickers in ', str(time.time() - start), ' seconds')

  def placeOrders(self, buyExchange, sellExchange, symbol:str, volume, buyPrice, sellPrice):

    base, quote = symbol.split('/')

    async def buy():
      logger.info(' * Start Buying ' + str(volume) + ' ' + symbol +  ' on ' + buyExchange.name)
  
      params = {}
      if(buyExchange.name is not 'Kraken') : 
        amount = buyExchange.amount_to_string(symbol, volume)
      else:
        amount = volume
    
      orderId = await self.tryMultipleTimesAsync(buyExchange, buyExchange.create_market_buy_order, 1, {'symbol': symbol, 'amount' : amount, 'params':params})
      
      if orderId is not None : logger.info(' * Create buy order with ID  ' + orderId['id'])
      else : logger.error(' * Could not create sell order on exchange ' + buyExchange.name)
    
      if base not in buyExchange.balance['free']: buyExchange.balance['free'][base] = 0
      previusBaseBalance = buyExchange.balance['free'][base]
      while buyExchange.balance['free'][base] < (previusBaseBalance + 0.99 * volume) :
        await asyncio.sleep(0.5)
        logger.info('check balance on ' + buyExchange.name)
        buyExchange.balance = await self.tryMultipleTimesAsync(buyExchange, buyExchange.fetch_balance)
    
      logger.info(' * End Buying ' + str(volume) + ' ' + symbol +  ' on ' + buyExchange.name) 

    async def sell():
      logger.info(' * Start Selling ' + str(volume) + ' ' + symbol +  ' on ' + sellExchange.name)

      params = {}
      if sellExchange.name is not 'Kraken' : 
        amount = sellExchange.amount_to_string(symbol, volume)
      else :
        amount = volume

      orderId = await self.tryMultipleTimesAsync(sellExchange, sellExchange.create_market_sell_order, 1, {'symbol': symbol, 'amount' : sellExchange.amount_to_string(symbol, amount), 'params':params})
      
      if orderId is not None : logger.info(' * Create sell order with ID  ' + orderId['id'])
      else : logger.error(' * Could not create sell order on exchange ' + sellExchange.name)
    
      previusBaseBalance = sellExchange.balance['free'][base]
      while sellExchange.balance['free'][base] > (previusBaseBalance - 0.99 * volume) :
        await asyncio.sleep(0.5)
        logger.info('check balance on ' + sellExchange.name)
        sellExchange.balance = await self.tryMultipleTimesAsync(sellExchange, sellExchange.fetch_balance)

      logger.info(' * End Selling ' + str(volume) + ' ' + symbol +  ' on ' + sellExchange.name)

    asyncio.ensure_future(buy())
    asyncio.ensure_future(sell())
    pending = asyncio.Task.all_tasks()
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*pending))


  def simulatePlaceOrder(self, arbitrage, volume):
    symbol = arbitrage['buy']['ticker']['symbol']
    buyTicker = arbitrage['buy']['exchange'].tickers[symbol]
    sellTicker = arbitrage['sell']['exchange'].tickers[symbol]
    base = arbitrage['base']
    quote = arbitrage['quote']

    logger.info("Simulating order for arbitrage : ")
    printArbitrage(arbitrage)

    # Wait a little bit
    time.sleep(0.5)

    # Refetch the tickers to see how the prices have evolved
    async def getLastTickerValues():
      input_coroutines = [asyncio.ensure_future(self.tryMultipleTimesAsync(arbitrage['buy']['exchange'], arbitrage['buy']['exchange'].fetch_ticker, 5, {'symbol': symbol})),
                          asyncio.ensure_future(self.tryMultipleTimesAsync(arbitrage['sell']['exchange'], arbitrage['sell']['exchange'].fetch_ticker, 5, {'symbol': symbol}))]
      tickers = await asyncio.gather(*input_coroutines)
      buyTicker[symbol] = tickers[0]
      sellTicker[symbol] = tickers[1]
  
    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(getLastTickerValues()))
  
    # Simulate balance change
    arbitrage['buy']['exchange'].balance['free'][base] += volume
    arbitrage['buy']['exchange'].balance['free'][quote] -= (volume * buyTicker['ask']*1.0026)
    arbitrage['sell']['exchange'].balance['free'][base] -= volume
    arbitrage['sell']['exchange'].balance['free'][quote] += (volume * sellTicker['bid']*0.9974)


  def isArbitrageSufficient(self, arbitrage):
    # define your own condition here might be complex: depending on markets, pairs, e.g.
    return arbitrage['profit'] > 2.5

  def CreateExchanges(self):
    # Création des exchanges
    exchanges = []

	# Add the desired exchanges here, for instance kraken and poloniex:
	
    kraken = ccxt.kraken({
      #'apiKey' : 'your key here', 
      #'secret':'your secret here'
      })
    exchanges.append(kraken)
    
    poloniex = ccxt.poloniex({
      #'apiKey' : 'your key here', 
      #'secret':'your secret here'
      })
    exchanges.append(poloniex)
	 
    self.loadMarkets(exchanges)

    # Assign default precision to avoid error on gdax 
    for exchange in exchanges:
      for symbol in exchange.symbols:
        if 'amount' not in exchange.markets[symbol]['precision']:
          exchange.markets[symbol]['precision']['amount'] = 8

    return exchanges
    

