from ibapi.errors import *
from ibapi.client import Contract
from dataclasses import dataclass, field

@dataclass
class RequestData:
    symbol: str
    time_frame: str
    bars: list = field(default_factory=list)

#Functions to create and merge all given contracts,

def get_contract(symbol_currency): #  contracts for same type of tickers
    contract = Contract()
    contract.symbol = symbol_currency[0:3]
    contract.secType = "CASH"
    contract.exchange = "IDEALPRO"
    contract.currency = symbol_currency[3:]
    
    return contract

def contracts(symbol): #creation of specific types of contracts and list with all Contract Objects to be returned 
    """
    Function that saves Contract ibapi objects for reqHisotoricalData function:
    
    - Retrieve a list with all existing contracts
    - New contracts that dont fit in given types, must be created
    and edit manually the .extend() function to append new contract objects
    -
    
    """
    # symbols = ["EURUSD"]
    contract = [get_contract(symbol)]

    xauusd = Contract()
    xauusd.symbol = "XAUUSD"
    xauusd.secType = "CMDTY"
    xauusd.exchange = "SMART"
    xauusd.currency = "USD"

    btc = Contract()
    btc.symbol = "BTC"
    btc.secType = "CRYPTO"
    btc.exchange = "PAXOS"
    btc.currency = "USD"

    #contracts.extend([xauusd, btc])

    return contract

#format to access contracts

'''
con = contracts()
for i, symbol in enumerate(con):
  print(con[i])
  con[i].symbol+con[i].currency
'''

#error list use when tws got disconnected. 
def connection_errors():
    """
    Error list for TWS connection errors
    """
    connection_errors = (CONNECT_FAIL, UPDATE_TWS, NOT_CONNECTED, UNKNOWN_ID,
                        UNSUPPORTED_VERSION, SOCKET_EXCEPTION, FAIL_CREATE_SOCK, SSL_FAIL)
    
    connection_error_codes = [error.code() for error in connection_errors]
    
    return connection_error_codes    

#error = connection_errors()
