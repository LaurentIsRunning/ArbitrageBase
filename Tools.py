import logging
import time
import asyncio
import ccxt
import ccxt.async as ccxt  # noqa: E402

#configure logging
logger = logging.getLogger("Arbitrage")


def addDictionaries(dict1:dict, dict2:dict) -> dict:
  keys = dict1.keys() | dict2.keys()
  result = dict()
  for k in keys:
    iskIn1 = k in dict1
    iskIn2 = k in dict2
    if iskIn1 and iskIn2:
      if (dict1[k] + dict2[k] != 0) : result[k] = dict1[k] + dict2[k]
    elif iskIn1:
      if dict1[k] != 0 : result[k] = dict1[k]
    elif iskIn2: 
      if dict2[k] != 0 : result[k] = dict2[k]
  return result

def substractDictionaries(dict1:dict, dict2:dict) -> dict:
  keys = dict1.keys() | dict2.keys()
  result = dict()
  for k in keys:
    iskIn1 = k in dict1
    iskIn2 = k in dict2
    if iskIn1 and iskIn2:
      if (dict1[k] - dict2[k] != 0) : result[k] = dict1[k] - dict2[k]
    elif iskIn1:
      if dict1[k] != 0 : result[k] = dict1[k]
    elif iskIn2: 
      if dict2[k] != 0 : result[k] = -dict2[k]
  return result



def tryMultipleTimes(func, maxRetries = 5, params:dict = None):
  result = None
  #print('Start ', func.__name__, ' ', description)

  for numRetries in range(0, maxRetries):
    try:  
        if params is None : result = func()
        else : result = func(**params)
        break
    except Exception as e:
        logger.error(func.__name__ + ':' + (type(e).__name__) + " : " + str(e))
    
  #print('End ', func.__name__, ' ', description)
  return result



def printArbitrage(arbitrage : dict):
  print(' -', "%.1f" % arbitrage['profit'], '-', str(arbitrage['buy']['ticker']['symbol']),': Buy on', str(arbitrage['buy']['exchange'].name),' [',str(arbitrage['buy']['ticker']['ask']),'] / sell on',str(arbitrage['sell']['exchange'].name),'[',str(arbitrage['sell']['ticker']['bid']),']')


def addVote(voteDict:dict, name:str, vote:float):
  if name in voteDict : voteDict[name] = vote + voteDict[name]
  else : voteDict[name] = vote



def areEqual(dict1:dict, dict2:dict):

  if len(dict1) != len(dict2) : return False

  for k in dict1:
    if dict1[k] != dict2[k] : return False

  return True

