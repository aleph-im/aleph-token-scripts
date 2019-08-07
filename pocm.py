import motor.motor_asyncio
import math
import yaml
import asyncio
from pprint import pprint
from datetime import date, datetime, timedelta
from common import get_sent_nuls, get_sent_tokens, transfer_packer, contract_call_packer
import pytz

START_DATE = date(2019,7,23)
CALC_TZ = pytz.FixedOffset(120)

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
    pprint(nodes)
    
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
    
    today = datetime.now(CALC_TZ).date()

    async for tx in txs:
        # block = await db.blocks.find_one({'height': tx['blockHeight']}, projection=['packingAddress'])
        packing = blocks[tx['blockHeight']]
        tx_time = datetime.utcfromtimestamp(tx['time']/1000).replace(tzinfo=pytz.utc)
        tx_date = tx_time.astimezone(CALC_TZ).date()
        
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
        
        async for contract_tx in db.transactions.find({
            'blockHeight': tx['blockHeight'],
            'type': 101
        }, projection=['info.sender', 'info.result.refundFee']):
            block_rewards[contract_tx['info']['sender']] -= contract_tx['info']['result']['refundFee']
        
        total_rewards = sum(block_rewards.values()) 
        others = total_rewards - block_rewards[reward_address]
        original_total = (others / 0.01)
        twenty_total = total_rewards - original_total
        total_twenties += twenty_total
        total_staked = total_rewards / (twenty_total/2000000000000)
        amounts_staked = {
            k: math.ceil((v / 0.01) / (twenty_total/2000000000000))
            for k, v in block_rewards.items()
            if (k != reward_address) and v > 0
        }
        
        amounts_staked[node['agent']] = amounts_staked.get(node['agent'], 0) + 2000000000000
        
        if twenty_total > 0:
            to_refund[node['agent']] = to_refund.get(node['agent'], 0) + int(twenty_total)
            
        for address, staked in amounts_staked.items():
            to_reward_shares[tx_date][address] = to_reward_shares[tx_date].get(address, 0) + staked
    
    for day, shares in to_reward_shares.items():
        day_amount = await get_period_value((day-start_date).days)
        if day == today:
            delta = datetime.now(CALC_TZ) - datetime.combine(day, datetime.min.time()).replace(tzinfo=CALC_TZ)
            day_amount = (delta/timedelta(days=1)) * day_amount
        print("day", day, day_amount)
        
        total_shares = sum(shares.values())
        for address, ashares in shares.items():
            to_distribute[address] = to_distribute.get(address, 0) + int(day_amount * (ashares/total_shares))
    
    return (to_refund, to_distribute)

    
async def main():
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
        
    client = motor.motor_asyncio.AsyncIOMotorClient(config.get('mongodb_host', 'localhost'),
                                                    config.get('mongodb_port', 27017))
    db = client.nulsmain
    
    to_refund, to_distribute = await get_distribution_info(config['reward_address'], START_DATE, db)
    pprint(to_refund)
    pprint(to_distribute)
    refunded = await get_sent_nuls(config['distribution_address'], db, remark=config['refund_remark'])
    pprint(refunded)
    to_refund = {
        addr: value - refunded.get(addr, 0)
        for addr, value in to_refund.items()
        if (value - refunded.get(addr, 0)) > 10000000
    }
    pprint(to_refund)
    distributed = await get_sent_tokens(config['source_address'], config['contract_address'], db, remark=config['distribution_remark'])
    pprint(distributed)
    to_distribute = {
        addr: value - distributed.get(addr, 0)
        for addr, value in to_distribute.items()
    }
    pprint(to_distribute)
    
    pri_key = bytes.fromhex(config['distribution_pkey'])
    
    # nutxo = None
    # if len(to_refund):
    #     # now let's do the refund.
    #     nutxo = await transfer_packer(config['distribution_address'],
    #                                   list(to_refund.items()),
    #                                   pri_key, remark=config['refund_remark'])
    #     print("refund issued for", to_refund)
    
    # distribution_list = [
    #     (address, value)
    #     for address, value in to_distribute.items()
    #     if value > (10**10)  # distribute more than 1 aleph only.
    # ]
    # # and the distribution.
    # max_items = config.get('bulk_max_items')
    # if len(distribution_list):
    #     for i in range(math.ceil(len(distribution_list) / max_items)):
    #         step_items = distribution_list[max_items*i:max_items*(i+1)]
    #         nutxo = await contract_call_packer(config['distribution_address'], config['contract_address'],
    #                                         'bulkTransferFrom', 
    #                                         [[config['source_address'],],
    #                                          [i[0] for i in step_items],
    #                                          [str(int(i[1])) for i in step_items]],
    #                                         pri_key, utxo=nutxo, remark=config['distribution_remark'],
    #                                         gas_limit=len(step_items)*30000)
    #         print("reward stage", i, len(step_items), "items")

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())