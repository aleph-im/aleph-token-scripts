import motor.motor_asyncio
import math
import yaml
import asyncio
import click
import copy
from pprint import pprint
from datetime import date, datetime, timedelta
from common import get_sent_nuls, get_sent_tokens, transfer_packer, contract_call_packer
from nuls2.api.server import get_server

START_DATE = date(2019,7,23)
ACTIVATION_THRESHOLD = 200000*(10**8)
CALCULATION_VALUE = 1000000*(10**8) # amounts of staked nuls to take as a base for calculation of rewards
DAY_AMOUNT = 100000*(10**10)

async def get_distribution_info(reward_address, start_date, db):
    register_txs = db.transactions.find({
        'type': 4,
        'txData.commissionRate': 99,
        'txData.rewardAddress': reward_address,
        'txData.deposit': 2000000000000
    }).sort('blockHeight')

    nodes = {
        tx['hash']: {
            'hash': tx['hash'],        
            'packing': tx['txData']['packingAddress'],        
            'commission': tx['txData']['commissionRate'],
            'agentId': tx['txData']['agentId'],
            'agent': tx['txData']['agentAddress'],
            'agentHash': tx['hash'],
            'stakers': {},
            'activated': False,
            'inactivation_height': -1,
            'activation_height': -1,
            'total_staked': 0
        }
        async for tx in register_txs
    }
    print(nodes)
    
    to_distribute = {}
    jointx_to_nodes = {}
    activated_nodes = list()
    
    async for tx in db.transactions.find({
            'type': {'$in': [5,6]}
        }, projection=['hash', 'txData', 'type', 'height', 'coinFroms']).sort('blockHeight'):
        if tx['type'] == 5:
            if tx['txData']['agentHash'] not in nodes:
                continue
            
            node = nodes[tx['txData']['agentHash']]
            # if node['activated']:
            #     continue # ignore already activated nodes
            
            node['total_staked'] += tx['txData']['amount']
            node['stakers'][tx['hash'][-16:]] = {
                'address': tx['txData']['address'],
                'height': tx['height'],
                'value': tx['txData']['amount']
            }
            jointx_to_nodes[tx['hash'][-16:]] = node['hash']
            print("[%s] %s staked %d (new total %d)" % (
                node['agentId'],            
                tx['txData']['address'],
                tx['txData']['amount'] / (10**8),
                node['total_staked'] / (10**8)))
            
            if (node['total_staked'] >= ACTIVATION_THRESHOLD
                and not node['activated']):
                print('Node %s activated!' % node['agentId'])
                node['activated'] = True
                node['activation_height'] = tx['height'] + 100 # approx 100 nodes per round
                activated_nodes.append(copy.deepcopy(node))
                
                
        if tx['type'] == 6:
            nonce = tx['coinFroms'][0]['nonce']
            amount = tx['coinFroms'][0]['amount']
            address = tx['coinFroms'][0]['address']
            if nonce not in jointx_to_nodes:
                continue
            
            node_hash = jointx_to_nodes[nonce]
            node = nodes[node_hash]
            
            # if node['activated']:
            #     continue # ignore already activated nodes
            
            node['total_staked'] -= amount
            del node['stakers'][nonce]
            
            print("[%s] %s unstaked %d (new total %d)" % (
                node['agentId'],            
                address,
                amount / (10**8),
                node['total_staked'] / (10**8)))
                
            if (node['total_staked'] < ACTIVATION_THRESHOLD
                and node['activated']):
                node['activated'] = False
                node['inactivation_height'] = tx['height']
            
    # for node in nodes.values():
    for node in activated_nodes:
        for staker_info in node['stakers'].values():
            from_height = max(node['inactivation_height'], staker_info['height'])
            minutes_waiting = (node['activation_height'] - from_height) / 6 # minutes
            day_ratio = minutes_waiting / 1440
            distribution_ratio = staker_info['value'] / CALCULATION_VALUE
            print("[%s] %s: %f of daily for %f days: %f aleph" % (
                node['agentId'], staker_info['address'], distribution_ratio, day_ratio, (DAY_AMOUNT*day_ratio*distribution_ratio)/(10**10)
            ))
            to_distribute[staker_info['address']] = \
                to_distribute.get(staker_info['address'], 0) + (DAY_AMOUNT*day_ratio*distribution_ratio)
    
    return to_distribute
                
        

async def rmain(config_file):
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        
    client = motor.motor_asyncio.AsyncIOMotorClient(config.get('mongodb_host', 'localhost'),
                                                    config.get('mongodb_port', 27017))
    db = client[config.get('mongodb_db', 'nuls2main')]
    
    to_distribute = await get_distribution_info(config['reward_address'], START_DATE, db)
    pprint(to_distribute)
    distributed = await get_sent_tokens(config['source_address'], config['contract_address'], db, remark=config['distribution_pre_active_remark'])
    pprint(distributed)
    # return
    to_distribute = {
        addr: value - distributed.get(addr, 0)
        for addr, value in to_distribute.items()
    }
    pprint(to_distribute)
    
    distribution_list = [
        (address, value)
        for address, value in to_distribute.items()
        if value > (10**10)  # distribute more than 1 aleph only.
    ]
    
    pri_key = bytes.fromhex(config['distribution_pkey'])
    # privkey = PrivateKey(pri_key, raw=True)
    # pub_key = privkey.pubkey.serialize()
    # address = await get_address(pub_key, config['chain_id'], config['prefix'])
    server = get_server(config['api_server'])
    
    pprint(distribution_list)
    # return
    # and the distribution.
    nonce = None
    max_items = config.get('bulk_max_items')
    if len(distribution_list):
        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items*i:max_items*(i+1)]
            nash = await contract_call_packer(
                server, config['distribution_address'], config['contract_address'],
                'bulkTransferFrom',
                [[config['source_address'],],
                 [i[0] for i in step_items],
                 [str(int(i[1])) for i in step_items]],
                pri_key, nonce=nonce, remark=config['distribution_pre_active_remark'],
                chain_id=config['chain_id'],
                asset_id=config.get('asset_id', 1),
                gas_limit=len(step_items)*30000)
            nonce = nash[-16:]
            await asyncio.sleep(10)
            
            print("reward stage", i, len(step_items), "items")
    

@click.command()
@click.option('--config', '-c', default='config.yaml', help='Config file')
def main(config):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(rmain(config))

if __name__ == '__main__':
    main()
