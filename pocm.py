import motor.motor_asyncio
import math
import yaml
import asyncio
from pprint import pprint
from datetime import date, datetime
from common import get_sent_nuls, get_sent_tokens

START_DATE = date(2019,7,18)

async def get_period_value(t, periods=(365*10), variance=0.5, total=200000000*(10**10)):
    middle = float(periods)/2
    mean = total/periods
    variance_ratio = (middle-t)/middle
    return mean+(mean*variance_ratio*variance)

async def get_distribution_info(reward_address, start_date, db):
    register_txs = db.transactions.find({
        'type': 4,
        'info.commissionRate': 99,
        'info.rewardAddress': reward_address,
        'info.deposit': 2000000000000
    })

    nodes = {
        tx['info']['packingAddress']: {
            'commission': tx['info']['commissionRate'],
            'agent': tx['info']['agentAddress']
        }
        async for tx in register_txs
    }
    
    blocks = {b['height']: b['packingAddress'] async for b in 
              db.blocks.find(
                    {'packingAddress': {'$in': list(nodes.keys())}},
                    projection=['height', 'packingAddress']
                )}

    txs = db.transactions.find({
        'type': 1,
        'outputs.address': reward_address
    }, projection=['hash', 'time', 'outputs.address', 'outputs.value', 'blockHeight', 'type'])

    total_stacked = {} # add amount stacked at each block minted... we will divide rewards based on that.
    total_twenties = 0

    to_reward_shares = {}
    to_refund = {}
    to_distribute = {}
    total_received = 0

    last_consensus = None

    async for tx in txs:
        # block = await db.blocks.find_one({'height': tx['blockHeight']}, projection=['packingAddress'])
        packing = blocks[tx['blockHeight']]
        tx_date = datetime.fromtimestamp(tx['time']/1000).date()
        
        if tx_date not in to_reward_shares:
            to_reward_shares[tx_date] = {}
        
        node = nodes.get(packing, None)
        if node is None:
            print("Erroneous packer found", packing, tx['hash'])
            continue
        
        block_rewards = {
            output['address']: output['value']
            for output in tx['outputs']
        }
        total_rewards = sum(block_rewards.values()) 
        others = total_rewards - block_rewards[reward_address]
        original_total = (others / 0.01)
        twenty_total = total_rewards - original_total
        total_twenties += twenty_total
        total_staked = total_rewards / (twenty_total/2000000000000)
        amounts_staked = {
            k: math.ceil((v / 0.01) / (twenty_total/2000000000000))
            for k, v in block_rewards.items()
            if k != reward_address
        }
        amounts_staked[node['agent']] = amounts_staked.get(node['agent'], 0) + 2000000000000
        to_refund[node['agent']] = to_refund.get(node['agent'], 0) + int(twenty_total)
        
        for address, staked in amounts_staked.items():
            to_reward_shares[tx_date][address] = to_reward_shares[tx_date].get(address, 0) + staked
    
    for day, shares in to_reward_shares.items():
        day_amount = await get_period_value((day-start_date).days)
        total_shares = sum(shares.values())
        for address, ashares in shares.items():
            to_distribute[address] = to_distribute.get(address, 0) + int(day_amount * (ashares/total_shares))
    
    return (to_refund, to_distribute)

    
async def main():
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
        
    client = motor.motor_asyncio.AsyncIOMotorClient('localhost', 27017)
    db = client.nulstest
    
    to_refund, to_distribute = await get_distribution_info(config['reward_address'], START_DATE, db)
    pprint(to_refund)
    pprint(to_distribute)
    refunded = await get_sent_nuls(config['distribution_address'], db, remark=config['refund_remark'])
    pprint(refunded)
    to_refund = {
        addr: value - refunded.get(addr, 0)
        for addr, value in to_refund.items()
    }
    pprint(to_refund)
    distributed = await get_sent_tokens(config['source_address'], config['contract_address'], db, remark=config['distribution_remark'])
    pprint(distributed)
    to_distribute = {
        addr: value - distributed.get(addr, 0)
        for addr, value in to_distribute.items()
    }
    pprint(to_distribute)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())