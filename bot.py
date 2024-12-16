from time import time

import json
import yaml
import re
import ccxt
import pprint
import sys
import argparse
import sqlite3
import time
import threading
import os
import signal
import requests

def create_table(cur):
  #cur.execute('DROP TABLE if exists orders')
  #cur.execute('DROP TABLE if exists profit')
  cur.execute('create table if not exists orders ( \
    id TEXT PRIMARY KEY, \
    timestamp DATETIME, \
    lastTradeTimestamp DATETIME, \
    symbol TEXT, \
    type TEXT, \
    side TEXT, \
    price NUMERIC, \
    amount NUMERIC, \
    status TEXT, \
    ccxt_order, TEXT, \
    other_created BOOLEAN NOT NULL DEFAULT 0 CHECK (other_created IN (0, 1)) \
  );')
  cur.execute('create table if not exists pairs ( \
    buy_id TEXT PRIMARY KEY, \
    sell_id TEXT UNIQUE\
    );')
  cur.execute('create table if not exists profit ( \
    profit NUMERIC, \
    timestamp DATETIME default CURRENT_TIMESTAMP, \
    symbol TEXT, \
    buy_price NUMERIC, \
    buy_amount NUMERIC, \
    buy_fee NUMERIC, \
    sell_price NUMERIC, \
    sell_amount NUMERIC, \
    sell_fee NUMERIC, \
    buy_id TEXT, \
    sell_id TEX, \
    PRIMARY KEY (buy_id, sell_id) \
    );')

  cur.execute("create view if not exists  buy_orders as select * from orders where side='buy';")
  cur.execute("create view if not exists sell_orders as select * from orders where side='sell';")

def update_profit(cur, symbol, sell_id):
  values = (sell_id,)
  cur.execute('select * from pairs where sell_id=?', values)
  pairs_row = cur.fetchone()
  if not pairs_row:
    return 0

  buy_id = pairs_row['buy_id']

  values = (sell_id,)
  cur.execute("select * from orders where id=? and status='closed'", values)
  sell_row = cur.fetchone()

  values = (buy_id,)
  cur.execute("select * from orders where id=? and status='closed'", values)
  buy_row = cur.fetchone()

  if sell_row and buy_row:
    sell_fee = 0
    for f in json.loads(sell_row['ccxt_order'])['fees']:
      if f['currency'] == config['quote']:
        sell_fee = sell_fee + f['cost']
      else:
        ticker = exchange.fetchTicker (f['currency']+"/"+config['quote'])
        sell_fee = ticker['last'] * f['cost']

    buy_fee = 0
    for f in json.loads(buy_row['ccxt_order'])['fees']:
      if f['currency'] == config['quote']:
        buy_fee = buy_fee + f['cost']
      else:
        ticker = exchange.fetchTicker (f['currency']+"/"+config['quote'])
        buy_fee = ticker['last'] * f['cost']

    #print ( "buy_fee =", buy_fee, "sell_fee =", sell_fee)

    profit = (sell_row['price'] * sell_row['amount']) - (buy_row['price'] * buy_row['amount']) - buy_fee - sell_fee
    values = (buy_id, sell_id, symbol, buy_row['price'], buy_row['amount'], buy_fee, \
              sell_row['price'], sell_row['amount'], sell_fee, profit)
    cur.execute('replace into profit (buy_id, sell_id, symbol, buy_price, buy_amount, buy_fee, \
                sell_price, sell_amount, sell_fee, profit) \
                values \
                (?,?,?,?,?,?,?,?,?,?) \
                ', values)
    return profit
  return 0

def update_all_profit(cur, symbol):
  cur.execute('select * from pairs')
  for row in cur.fetchall():
    update_profit(cur, symbol, row['sell_id'])

def my_buy_order(symbol, amount, price, params, skip_bal_check=False):
  ret = False
  s = symbol.split('/')
  base = s[0]
  quote = s[1]
  try:
    free = balances['free'][quote]
  except:
    free = 0
  line = 'base=' + "%5s" % base + ' amount=' + "%10.4f" % amount + ' price=' + "%10.4f" % price + " free=" + "%10.4f" % free
  if free >= (amount*price) or skip_bal_check:
    try:
      ret = exchange.create_limit_buy_order (symbol, amount, price, params)
      print ( ' BUY: ' + line )
    except Exception as e:
      print ( ' ERROR BUY: ' + line )
      print(e)
      ret = False
  else:
    print ( ' NOT ENOUGH BALANCE FOR BUY: ' + line )
  return ret

def my_sell_order(symbol, amount, price, params, skip_bal_check=False):
  ret = False
  s = symbol.split('/')
  base = s[0]
  try:
    free = balances['free'][base]
  except:
    free = 0
  line = 'base=' + "%5s" % base + ' amount=' + "%10.4f" % amount + ' price=' + "%10.4f" % price + " free=" + "%10.4f" % free
  if free >= amount or skip_bal_check:
    try:
      ret = exchange.create_limit_sell_order (symbol, amount, price, params)
      print ( 'SELL: ' + line )
    except Exception as e:
      print ( 'ERROR SELL: ' + line )
      print(e)
      ret = False
  else:
    print ( 'NOT ENOUGH BALANCE FOR SELL: ' + line )
  return ret

def insert_order(cur, order):
  values = ( order['id'], order['timestamp'],order['symbol'],order['type'],order['side'], \
    order['price'],order['amount'],order['status'] )
  result = cur.execute('INSERT INTO orders (id,timestamp,symbol,type,side, \
    price,amount, status) VALUES(?,?,?,?,?,?,?,?)', values)

def update_order(cur, order):
  ccxt_order = json.dumps(order)
  values = ( order['timestamp'], order['lastTradeTimestamp'], order['symbol'], order['type'], order['side'], order['price'], \
             order['amount'], order['status'], ccxt_order, order['id'] )
  cur.execute('UPDATE orders SET timestamp=?, lastTradeTimestamp=?, symbol=?, type=?, side=?, price=?, \
               amount=?, status=?,  ccxt_order=? WHERE id=?', values)

def print_order(order, msg='', notify=False):
  o={}
  o['side'] = order['side']
  o['id'] = order['id']
  o['amount'] = order['amount']
  o['price'] = order['price']
  o['status'] = order['status']
  o['timestamp'] = order['timestamp']
  o['lastTradeTimestamp'] = order['lastTradeTimestamp']
  msg2 = msg + ' Order: ' + str(o)
  #print(msg + ' Order:', o)
  print(msg2)
  if notify:
    ntfy(msg2)

def update_orders_table(cur, symbol):
  exchange_id = config['exchange']['exchange_id']
  s = int(time.time()*1000) - (31*24*3600*1000)
  l = 100

  check={}
  if exchange.has['fetchOrders']:
    orders = exchange.fetchOrders(symbol=symbol, since=s, limit=l)
    for order in orders:
      update_order(cur, order)
      check[order['id']] = True
  else:
    open_orders = exchange.fetchOpenOrders(symbol=symbol, since=s, limit=l)
    closed_orders =  exchange.fetchClosedOrders(symbol=symbol, since=s, limit=l)
    canceled_orders = exchange.fetchCanceledOrders(symbol=symbol, since=s, limit=l)
    for order in open_orders+closed_orders+canceled_orders:
      update_order(cur, order)
      check[order['id']] = True

  cur.execute("select * from orders where status='open' order by timestamp")
  for row in cur.fetchall():
    try:
      check[row['id']]
    except:
      if exchange_id == 'bybit':
        p={"acknowledged": True}
      else:
        p={}
      order = exchange.fetchOrder(id=row['id'], symbol=symbol, params=p)
      print_order(order,'CHECK')
      update_order(cur, order)

def cancel_orders(symbol):
  orders = exchange.fetchOpenOrders (symbol)
  toCancel = []
  for order in orders:
    toCancel.append(order['id'])
  if len(toCancel) > 0:
    print("Canceling all orders")
    #pprint.pp(toCancel)
    for id in toCancel:
      result = exchange.cancelOrder(id=id, symbol=symbol)
      #print("result")
      #pprint.pp(result)

def avg_price(cur, symbol):
  values = ( symbol, )
  cur.execute('select avg(price) avg from orders where symbol=? and status="closed"', values )
  rows = cur.fetchone()
  if rows[0]:
    ret = rows[0]
  else:
    values = ( symbol, )
    try:
      cur.execute('select price from orders where symbol=? and status="open" and side="buy" order by price DESC', values)
      buy_row = cur.fetchone()
    except:
      buy_row = None
    try:
      cur.execute('select price from orders where symbol=? and status="open" and side="sell" order by price', values)
      sell_row = cur.fetchone()
    except:
      sell_row = None
    if buy_row and sell_row:
      ret = (buy_row[0]+sell_row[0]) / 2
    else:
      ticker = exchange.fetchTicker (symbol)
      ret = (ticker['bid'] + ticker['ask']) / 2
    #print('ret', ret)
  return ret

def dict_from_row(row):
    return dict(zip(row.keys(), row))

def get_buy_rows(cur,symbol):
  values = ( symbol, )
  cur.execute('select * from orders where symbol=? and status="open" and side="buy" order by price', values)
  return cur.fetchall()

def get_sell_rows(cur, symbol):
  values = ( symbol, )
  cur.execute('select * from orders where symbol=? and status="open" and side="sell" order by price', values)
  return cur.fetchall()

def in_range(cur, symbol):
  ticker = exchange.fetchTicker (symbol)

  sell_rows = get_sell_rows(cur, symbol)
  if len(sell_rows) > 0:
    lowest_sell_price = sell_rows[0]['price']
    highest_sell_price = sell_rows[len(sell_rows)-1]['price']
  else:
    return False
  buy_rows = get_buy_rows(cur, symbol)
  if len(buy_rows) > 0:
    loweset_buy_price = buy_rows[0]['price']
    highest_buy_price = buy_rows[len(buy_rows)-1]['price']
  else:
    return False

  if ticker['ask'] > highest_sell_price:
    return False

  if ticker['bid'] < loweset_buy_price:
    return False

  return True

def check_grid(cur, symbol):
  ret = True
  avg_ticker = (ticker['bid'] + ticker['ask']) / 2
  buy_rows = get_buy_rows(cur, symbol)
  if len(buy_rows) > 0:
    loweset_buy_price = buy_rows[0]['price']
    highest_buy_price = buy_rows[len(buy_rows)-1]['price']
  else:
    loweset_buy_price = avg_ticker
    highest_buy_price = avg_ticker

  sell_rows = get_sell_rows(cur, symbol)
  if len(sell_rows) > 0:
    lowest_sell_price = sell_rows[0]['price']
    highest_sell_price = sell_rows[len(sell_rows)-1]['price']
  else:
    lowest_sell_price = avg_ticker
    highest_sell_price = avg_ticker

  diff = lowest_sell_price - highest_buy_price
  max_diff = 2.5*(lowest_sell_price/100*config['grid_percentage'])
  if diff > max_diff:
    print ("Missing orders in middle of grid")
    ret = False
  
  prev = None
  for row in reversed(buy_rows):
    if prev:
      diff = prev['price'] - row['price']
      max_diff = (prev['price']/100*config['grid_percentage']*1.5)
      #print('diff=', diff, 'max_diff=', max_diff, 'prev=', prev['price'], 'row=', row['price'])
      if diff > max_diff: 
        print('Missing buy row')
        ret = False
    prev=row

  prev = None
  for row in sell_rows:
    if prev:
      diff = row['price'] - prev['price']
      max_diff = (prev['price']/100*config['grid_percentage']*1.5)
      #print('diff=', diff, 'max_diff=', max_diff, 'prev=', prev['price'], 'row=', row['price'])
      if diff > max_diff: 
        print('Missing sell row, diff=', diff)
        ret = False
    prev=row

  return ret

def print_grid(cur, symbol):
  sell_rows = get_sell_rows(cur, symbol)
  buy_rows = get_buy_rows(cur, symbol)

  prev = None
  c=len(sell_rows)
  for row in reversed(sell_rows):
    if prev:
      diff = prev['price'] - row['price']
    else:
      diff = 0
    print ( 'SELL ROW ' + "%2d" % c + ': amount=' + "%10.6f" % row['amount'] + ' price=' + "%10.6f" % row['price'] + " diff=" + "%10.8f" % diff )
    prev=row
    c=c-1

  #prev = None
  c=0
  for row in reversed(buy_rows):
    c=c+1
    if prev:
      diff = prev['price'] - row['price']
    else:
      diff = 0
    print ( ' BUY ROW ' + "%2d" % c + ': amount=' + "%10.6f" % row['amount'] + ' price=' + "%10.6f" % row['price'] + " diff=" + "%10.8f" % diff )
    prev=row

def create_other_orders(cur, symbol):
  s = int(time.time()-300)*1000
  values=(s,)
  cur.execute('select * from orders where status = "closed" and other_created=0 and lastTradeTimestamp > ? order by lastTradeTimestamp', values)
  rows=cur.fetchall()
  order_created = False
  for r in rows:
    if r['side'] == 'sell':
      print_order(r,'CLOSED', True)
      #price = r['price'] - (r['price']/100*config['grid_percentage'])
      price = r['price'] - (r['price']/(100-config['grid_percentage'])*config['grid_percentage'])
      in_quote = r['price']*r['amount']
      profit = update_profit(cur, symbol, r['id'])
      amount = (in_quote-profit)/price
      order = my_buy_order (symbol, amount, price, { }, True)
      if order:
        values=(r['id'],)
        cur.execute('update orders set other_created=1 WHERE id=?', values)
        insert_order(cur, order)
        order_created = True
#      else:
#        values=(r['id'],)
#        cur.execute('update orders set other_created=1 WHERE id=?', values)
    if r['side'] == 'buy':
      print_order(r,'CLOSED', True)
      #price = r['price'] + (r['price']/100*config['grid_percentage'])
      price = r['price'] + (r['price']/(100-config['grid_percentage'])*config['grid_percentage'])
      amount = r['amount']
      order = my_sell_order (symbol, amount, price, { }, True)
      if order:
        values=(r['id'],)
        cur.execute('update orders set other_created=1 WHERE id=?', values)
        insert_order(cur, order)
        values = (r['id'], order['id'])
        con.execute('insert into pairs (buy_id, sell_id) values (?,?)', values)
        order_created = True
#      else:
#        values=(r['id'],)
#        cur.execute('update orders set other_created=1 WHERE id=?', values)
  if len(rows) > 0:
    update_orders_table(cur, symbol)


def update_balances_ticker(symbol):
  global balances
  global ticker
  global total_quote
  global prev_order_amount
  balances = exchange.fetchBalance()
  ticker = exchange.fetchTicker (symbol)
  tickers = exchange.fetchTickers ()

  total_quote=0
  for k in balances['total']:
    if balances['total'][k] > 0:
      if k == config['quote']:
        total_quote = total_quote + balances['total'][k]
      else:
        t = k+'/'+config['quote']
        last = tickers[t]['last']
        total_quote = total_quote + (balances['total'][k] * last)
  try:
    config['order_amount_perc']
    config['order_amount'] = (total_quote/100) * config['order_amount_perc']
  except:
    pass
  try:
    prev_order_amount
  except:
    prev_order_amount = 0
  if config['order_amount'] > prev_order_amount*1.01 or config['order_amount'] < prev_order_amount*0.99:
    prev_order_amount = config['order_amount']
    print('order_amount=', config['order_amount'])

def add_missing(cur, symbol):
  sell_rows = get_sell_rows(cur, symbol)
  buy_rows = get_buy_rows(cur, symbol)

  if len(sell_rows) == 0 and len(buy_rows) == 0:
    print('No buy and sell rows')
    return False

  i=len(sell_rows)
  if i > config['grid_up']:
    result = exchange.cancelOrder(id=sell_rows[i-1]['id'], symbol=symbol)
  else:
    if i == 0:
      sell_price = buy_rows[len(buy_rows)-1]['price']
      sell_price = sell_price + (sell_price/100*config['grid_percentage'])
      print('CHECK: sell_price=', sell_price)
    else:
      sell_price = sell_rows[i-1]['price']
    while i<config['grid_up']:
      sell_price = sell_price + (sell_price/100*config['grid_percentage'])
      i=i+1
      amount = config['order_amount']/sell_price
      amount = amount + (amount/100*config['grid_percentage'])
      order = my_sell_order (symbol, amount, sell_price, { })
      if order:
        insert_order(cur, order)
      else:
        break

  i=len(buy_rows)
  if i > config['grid_down']:
    result = exchange.cancelOrder(id=buy_rows[0]['id'], symbol=symbol)
  else:
    if i == 0:
      buy_price = sell_rows[0]['price']
      buy_price = buy_price - (buy_price/100*config['grid_percentage'])
      print('CHECK: buy_price=', buy_price)
    else:
      buy_price = buy_rows[0]['price']
    while i<config['grid_down']:
      buy_price = buy_price - (buy_price/100*config['grid_percentage'])
      i=i+1
      amount = config['order_amount']/buy_price
      order = my_buy_order (symbol, amount, buy_price, { })
      if order:
        insert_order(cur, order)
      else:
        break

def cancel_and_create_orders(cur, symbol):
  print("Canceling all orders and create fresh grid")
  cancel_orders(symbol)
  cur.execute("update orders set status='unknown' where status = 'open'")
  cur.execute("update orders set other_created=1")
  avg_orders = avg_price(cur, symbol)
  avg_ticker = (ticker['bid'] + ticker['ask']) / 2

  max_down = avg_orders-(avg_orders/100*config['max_distance_down'])
  max_up = avg_orders+(avg_orders/100*config['max_distance_up'])

  print('avg_orders='+str(avg_orders)+' avg_ticker='+str(avg_ticker)+" max_down="+str(max_down)+" max_up="+str(max_up))

  buy_price = avg_ticker
  for i in range(config['grid_down']):
    buy_price = buy_price - (buy_price/100*config['grid_percentage'])
    if buy_price < max_down:
      break
    print("i="+str(i)+" buy_price="+str(buy_price))
    
    amount = config['order_amount']/buy_price
    order = my_buy_order (symbol, amount, buy_price, { })
    if order:
      insert_order(cur, order)
    else:
      break

  sell_price = avg_ticker
  for i in range(config['grid_up']):
    sell_price = sell_price + (sell_price/100*config['grid_percentage'])
    if sell_price > max_up:
      break
    print("i="+str(i)+" sell_price="+str(sell_price))
    
    amount = config['order_amount']/sell_price
    amount = amount + (amount/100*config['grid_percentage'])
    order = my_sell_order (symbol, amount, sell_price, { })
    if order:
      insert_order(cur, order)
    else:
      break

def load_config():
  global config
  fd = open(args.config)
  config = yaml.safe_load(fd)
  fd.close()

  try: config['minimal_market_buy']
  except: config['minimal_market_buy'] = 10

  try: config['order_amount']
  except: config['order_amount'] = 50

  try: config['max_distance_up']
  except: config['max_distance_up'] = 20

  try: config['max_distance_down']
  except: config['max_distance_down'] = config['max_distance_up']

  try: config['grid_up']
  except: config['grid_up'] = 5

  try: config['grid_down']
  except: config['grid_down'] = config['grid_up']

  try: config['grid_percentage']
  except: config['grid_percentage'] = 1

  #pprint.pp(config)

def printf(format, *args):
  print(format % args)

def print_summary(cur, symbol):
  global prev_profit
  print('')
  r = {}
  r[config['base']] = {}
  r[config['quote']] = {}
  for a in ('free', 'used', 'total'):
    bal = "%12.5f" % (balances[config['base']][a])
    in_quote = "%7.2f" % (ticker['last'] * balances[config['base']][a])
    r[config['base']]["%5s" % a] = { config['base']: bal, config['quote']: in_quote }
  for a in ('free', 'used', 'total'):
    bal = "%12.5f" % (balances[config['quote']][a])
    r[config['quote']]["%5s" % a] = { config['quote']: bal }
  pprint.pp(r, width=200)
  #print("Balances for " + config['quote'] + ":", balances[config['quote']])
  fee = exchange.fetchTradingFee(symbol, {})
  try:
    del fee['info']['capabilities']
  except:
    pass
  pprint.pp(fee['info'], width=200)

  values=(symbol,)
  cur.execute("select sum(profit) from profit where symbol=?;", values)
  row = cur.fetchone()
  try: prev_profit
  except: prev_profit = 0
  if row[0] != prev_profit:
    msg = 'Profit for ' + config['base'] + '/' + config['quote'] + " on " + config['exchange']['exchange_id'] + ":",  row[0], config['quote']
    print (msg)
    ntfy (msg)
  prev_profit = row[0]

def shutdown():
  print ('Shutting down')
  cancel_orders(symbol)
  os._exit(1)

def interrupt_handler(signum, frame):
    print(f'Handling signal {signum} ({signal.Signals(signum).name}).')
    try:
      t.cancel()
    except:
      pass
    sys.exit(0)

def ntfy(msg):
  requests.post("https://ntfy.sh/scb", data=str(msg).encode(encoding='utf-8'))

signal.signal(signal.SIGINT, interrupt_handler)
signal.signal(signal.SIGTERM, interrupt_handler)

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', default='config.yaml')
args = parser.parse_args()

load_config()

exchange_id = config['exchange']['exchange_id']
exchange_class = getattr(ccxt, exchange_id)
exchange = exchange_class({
  'apiKey': config['exchange']['key'],
  'secret': config['exchange']['secret'],
})

symbol = config['base'] + '/' + config['quote']

ntfy('Start SCP symbol='+symbol)

con = sqlite3.connect(exchange_id + "-" + config['base'] + "-" + config['quote'] + ".db", isolation_level=None)
con.row_factory = sqlite3.Row
cur = con.cursor()
create_table(cur)

update_balances_ticker(symbol)
update_all_profit(cur, symbol)
update_orders_table(cur, symbol)

if not in_range(cur, symbol):
  cancel_and_create_orders(cur, symbol)
  update_orders_table(cur, symbol)

prev=0
print("Let's go")
print_grid(cur, symbol)
print_summary(cur, symbol)
grid_errors = 0
while True:
  try:
    t.cancel()
  except:
    pass
  t = threading.Timer(120, shutdown)
  t.start()
  update_balances_ticker(symbol)
  update_orders_table(cur, symbol)
  create_other_orders(cur, symbol)
  add_missing(cur, symbol)
  if check_grid(cur, symbol):
    grid_errors = 0
  else:
    grid_errors = grid_errors + 1
    print ('grid_errors=', grid_errors)

  if grid_errors > 5:
    print('errors in grid, exiting')
    cancel_and_create_orders(cur, symbol)
    grid_errors = 0
    time.sleep(60)
  
  print_grid(cur, symbol)
  print_summary(cur, symbol)
  print('')
  time.sleep(5)
  #print('')
  #sys.exit()
