import motor.motor_asyncio
import math
import yaml
import asyncio
from pprint import pprint
from datetime import date, datetime, timedelta
from common import get_sent_nuls, get_sent_tokens, transfer_packer, contract_call_packer

START_DATE = date(2019,7,23)
ACTIVATION_THRESHOLD = 200000*(10**8)
CALCULATION_VALUE = 1000000*(10**8) # amounts of staked nuls to take as a base for calculation of rewards
DAY_AMOUNT = 82000*(10**10)*4

async def get_distribution_info(reward_address, start_date, db):
    register_txs = db.transactions.find({
        'type': 4,
        'info.commissionRate': 99,
        'info.rewardAddress': reward_address,
        'info.deposit': 2000000000000
    }).sort('blockHeight')

    nodes = {
        tx['hash']: {
            'packing': tx['info']['packingAddress'],        
            'commission': tx['info']['commissionRate'],
            'agent': tx['info']['agentAddress'],
            'agentHash': tx['hash']
        }
        async for tx in register_txs
    }
    
    to_distribute = {}
    
    for node_hash, node_info in nodes.items():
        print("node %s" % node_info['agent'])
        join_txs = db.transactions.find({
            'type': {'$in': [5,6]},
            'info.agentHash': node_hash
        }).sort('blockHeight')
        total_staked = 0
        node_stakers = {}
        activated = False
        activation_height = None
        async for join_tx in join_txs:
            if join_tx['type'] == 5:
                total_staked += join_tx['info']['deposit']
                node_stakers[join_tx['hash']] = {
                    'address': join_tx['info']['address'],
                    'height': join_tx['blockHeight'],
                    'value': join_tx['info']['deposit']
                }
                print("%s staked %d (new total %d)" % (
                    join_tx['info']['address'],
                    join_tx['info']['deposit'] / (10**8),
                    total_staked / (10**8)))
            elif join_tx['type'] == 6:
                total_staked -= join_tx['inputs'][0]['value']
                del node_stakers[join_tx['info']['joinTxHash']]
                print("%s unstaked %d (new total %d)" % (
                    join_tx['inputs'][0]['address'],
                    join_tx['inputs'][0]['value'] / (10**8),
                    total_staked / (10**8)))
            if total_staked >= ACTIVATION_THRESHOLD:
                print("node %s activated" % node_info['agent'])
                activated = True
                activation_height = join_tx['blockHeight'] + 100 # approx 100 nodes per round
                break
        
        if activated: # don't give bonus on non activated nodes yet
            for staker_info in node_stakers.values():
                minutes_waiting = (activation_height - staker_info['height']) / 6 # minutes
                day_ratio = minutes_waiting / 1440
                distribution_ratio = staker_info['value'] / CALCULATION_VALUE
                print("%s: %f of daily for %f days: %f aleph" % (
                    staker_info['address'], distribution_ratio, day_ratio, (DAY_AMOUNT*day_ratio*distribution_ratio)/(10**10)
                ))
                to_distribute[staker_info['address']] = \
                    to_distribute.get(staker_info['address'], 0) + (DAY_AMOUNT*day_ratio*distribution_ratio)
    
    return to_distribute
                
        

    
async def main():
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
        
    client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    db = client.nulsmain
    to_distribute = await get_distribution_info(config['reward_address'], START_DATE, db)
    pprint(to_distribute)
    distributed = await get_sent_tokens(config['source_address'], config['contract_address'], db, remark=config['distribution_pre_active_remark'])
    pprint(distributed)
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
    
    # and the distribution.
    nutxo = None
    max_items = config.get('bulk_max_items')
    if len(distribution_list):
        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items*i:max_items*(i+1)]
            nutxo = await contract_call_packer(config['distribution_address'], config['contract_address'],
                                            'bulkTransferFrom', 
                                            [[config['source_address'],],
                                             [i[0] for i in step_items],
                                             [str(int(i[1])) for i in step_items]],
                                            pri_key, utxo=nutxo, remark=config['distribution_pre_active_remark'],
                                            gas_limit=len(step_items)*30000)
            print("reward stage", i, len(step_items), "items")
    


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())