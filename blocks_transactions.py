########################################################################################################################
# # To run below code on your local environment, Please install QT wallet / RPC c0ban node.                          # #
# # See more information here:                                                                                       # #
# #                              https://www.c0ban.co/                                                               # #
# #                              https://github.com/c0ban/c0ban/releas                                               # #
# #                                                                                                                  # #
########################################################################################################################

import datetime as dt
start_time = dt.datetime.now()
import time
import warnings
from termcolor import colored
warnings.filterwarnings('ignore')
import pandas as pd
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

#----------------------------------------------------------------------------------------------------------------------
rpc_user= 'Your RPC USER NAME'
rpc_password='Your RPC PASSWORD'
block = pd.DataFrame(columns = ['height', 'version', 'hash', 'size', 'strippedsize','weight','time', 'nonce', 'bits', 'reward', 'difficulty', 'tx'])
transaction = pd.DataFrame(columns = ['hash', 'size', 'version', 'input', 'output', 'block_hash','block_number', 'block_time'])

rpc = AuthServiceProxy("http://%s:%s@localhost:9999"%(rpc_user, rpc_password))
end_block =rpc.getblockcount() # use to get all blocks
start_block = 1
Block = []
Transaction = []
for i in range(start_block ,end_block):
    try:
        b = (rpc.getblock(rpc.getblockhash(i)))
        for j in range(len(b['tx'])):
            t = rpc.getrawtransaction(b['tx'][j],1)
            Transaction.append(t)
            Block.append(b)
        if i%100000 ==0:
            print(colored(i, 'blue','on_grey', attrs=['bold']),'blocks processed at', colored(dt.datetime.now(), 'blue','on_grey', attrs=['bold']))
    except ConnectionResetError:
        time.sleep(500)
        b = (rpc.getblock(rpc.getblockhash(i)))
        for j in range(len(b['tx'])):
            t = rpc.getrawtransaction(b['tx'][j],1)
            Transaction.append(t)
            Block.append(b)
        if i%100000 ==0:
            print(colored(i, 'blue','on_grey', attrs=['bold']),'blocks processed at', colored(dt.datetime.now(), 'blue','on_grey', attrs=['bold']))
# Convert json to DataFrame
Block = pd.DataFrame(Block)

block = Block[['height', 'version', 'hash', 'size', 'strippedsize','weight','time', 'nonce', 'bits', 'reward', 'difficulty', 'tx']]
block['transaction_count'] = block['tx'].apply(lambda x: len(x))
block['time'] = block.time.apply(lambda x: dt.datetime.fromtimestamp(x))

Transaction = pd.DataFrame(Transaction)
transaction = Transaction[['hash', 'size', 'version', 'vin', 'vout', 'blockhash','height', 'blocktime']]

# Save blocks and transactions data as csv file
block.to_csv('blocks.csv', index = False)
transaction.to_csv('transactions.csv', index = False)

end_time = dt.datetime.now()
print('Total processing time:',end_time - start_time)
print(colored('We are done.Thank you for patience.😄', 'blue', 'on_grey', attrs =['bold']))
print("\033[1;34;40m")

