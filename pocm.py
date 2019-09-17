import motor.motor_asyncio
import math
import yaml
import asyncio
import click
from pprint import pprint
from datetime import date, datetime, timedelta
from common import get_sent_nuls, get_sent_tokens, transfer_packer, contract_call_packer, get_address
import pytz
from nuls2.api.server import get_server
from secp256k1 import PrivateKey

START_DATE = date(2019,7,23)
CALC_TZ = pytz.FixedOffset(120)

async def get_period_value(t, periods=(365*5), variance=0.5, total=100000000*(10**10)):
    middle = float(periods)/2
    mean = total/periods
    variance_ratio = (middle-t)/middle
    return mean+(mean*variance_ratio*variance)

async def get_distribution_info(reward_address, start_date, db,
                                bonus_period=60, bonus_members=100,
                                bonus_multiplier=1.15,
                                node_commission=0.1):
    register_txs = db.transactions.find({
        'type': 4,
        'txData.commissionRate': 99,
        'txData.rewardAddress': reward_address,
        'txData.deposit': 2000000000000
    })

    nodes = {
        tx['txData']['packingAddress']: {
            'commission': tx['txData']['commissionRate'],
            'agent': tx['txData']['agentAddress']
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
        'coinTos.address': reward_address,
        'coinTos.symbol': 'NULS'
    }, projection=['hash', 'createTime', 'coinTos.address', 'coinTos.amount', 'height', 'type'])

    total_stacked = {} # add amount stacked at each block minted... we will divide rewards based on that.
    total_twenties = 0

    to_reward_shares = {}
    to_refund = {}
    to_distribute = {}
    targets = []
    total_received = 0

    last_consensus = None
    
    today = datetime.now(CALC_TZ).date()

    async for tx in txs:
        # block = await db.blocks.find_one({'height': tx['blockHeight']}, projection=['packingAddress'])
        if tx['height'] not in blocks:
            print('non existing block %d info, skipping' % tx['height'])
            continue
        
        packing = blocks[tx['height']]
        tx_time = datetime.utcfromtimestamp(tx['createTime']).replace(tzinfo=pytz.utc)
        tx_date = tx_time.astimezone(CALC_TZ).date()
        
        if tx_date not in to_reward_shares:
            to_reward_shares[tx_date] = {}
        
        node = nodes.get(packing, None)
        if node is None:
            print("Erroneous packer found", packing, tx['hash'])
            continue
        
        block_rewards = {
            output['address']: output['amount']
            for output in tx['coinTos']
        }
        
        # async for contract_tx in db.transactions.find({
        #     'blockHeight': tx['blockHeight'],
        #     'type': 101
        # }, projection=['info.sender', 'info.result.refundFee']):
        #     block_rewards[contract_tx['info']['sender']] -= contract_tx['info']['result']['refundFee']
        
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
            if address not in targets:
                targets.append(address)
                
            if address == node['agent']:
                staked = staked + ((total_staked-2000000000000) * node_commission)
            else:
                staked = staked * (1-node_commission)
                
            to_reward_shares[tx_date][address] = to_reward_shares[tx_date].get(address, 0) + staked
    
    first_stakers = targets[:bonus_members]
    i = 0
    for day, shares in to_reward_shares.items():
        i += 1
        day_amount = await get_period_value((day-start_date).days)
        if day == today:
            delta = datetime.now(CALC_TZ) - datetime.combine(day, datetime.min.time()).replace(tzinfo=CALC_TZ)
            day_amount = (delta/timedelta(days=1)) * day_amount
        print("day", day, day_amount)
        
        total_shares = sum(shares.values())
        for address, ashares in shares.items():
            addr_day_amount = int(day_amount * (ashares/total_shares))
            if (i <= bonus_period) and (address in first_stakers):
                addr_day_amount = addr_day_amount * bonus_multiplier
                
            to_distribute[address] = to_distribute.get(address, 0) + addr_day_amount
    
    return (to_refund, to_distribute)

    
async def rmain(config_file):
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        
    client = motor.motor_asyncio.AsyncIOMotorClient(config.get('mongodb_host', 'localhost'),
                                                    config.get('mongodb_port', 27017))
    db = client[config.get('mongodb_db', 'nuls2main')]
    
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
    # address = await get_address(pub_key, config['chain_id'], config['prefix'])
    # pri_key = bytes.fromhex(config['source_pkey'])
    privkey = PrivateKey(pri_key, raw=True)
    pub_key = privkey.pubkey.serialize()
    address = await get_address(pub_key, config['chain_id'], config['prefix'])
    server = get_server(config['api_server'])
    
    nonce = None
    # nutxo = None
    if len(to_refund):
        # now let's do the refund.
        for refund in to_refund.items():
            nash = await transfer_packer(server, config['distribution_address'],
                                  [refund], pri_key, nonce=nonce,
                                  remark=config['refund_remark'],
                                  chain_id=config['chain_id'],
                                  asset_id=config.get('asset_id', 1))
            nonce = nash[-16:]
            await asyncio.sleep(2)
        print("refund issued for", to_refund)
    
    distribution_list = [
        (address, value)
        for address, value in to_distribute.items()
        if value > (10**10)  # distribute more than 1 aleph only.
    ]
    # return
    pprint(to_distribute.keys())
    print([str(v) for v in to_distribute.values()])
    # # and the distribution.
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
                pri_key, nonce=nonce, remark=config['distribution_remark'],
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
