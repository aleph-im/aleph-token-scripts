import asyncio
import json
import time
import math
import yaml
import click
from secp256k1 import PrivateKey

from common import contract_call_packer, broadcast, get_address, nuls1to2
from nuls2.api.server import get_server



    


async def rmain(config_file):
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        
    pri_key = bytes.fromhex(config['source_pkey'])
    privkey = PrivateKey(pri_key, raw=True)
    pub_key = privkey.pubkey.serialize()
    # address = await get_address(pub_key, config['network_id'])
    address = await get_address(pub_key, config['chain_id'], config['prefix'])
    print(address)
    server = get_server(config['api_server'])

    addresses = []
    payload = json.load(open('./nuls1-aleph-holders-snapshot.json', 'r'))
    holders = {h['_id']: h['balance'] for h in payload['holders'][1:]}
    print(holders)
    
    for address, balance in holders.items():
        print(address, nuls1to2(address, config['chain_id'], config['prefix']), balance / (10**10))
    # for line in open('/home/jon/Documents/Aleph/snapshots/cached_unspent-testnet-20190527.json', 'r'):
    #     val = json.loads(line)
    #     if val['_id'].startswith('TT'):
    #         unspent = val['unspent_value']
    #         if isinstance(unspent, dict):
    #             unspent = int(unspent['$numberLong'])
            
    #         if unspent >= 99500000:
    #             addresses.append(val['_id'])

    # nutxo = None
#    for address in addresses[:50]:
#        nutxo = await nuls_packer([address],
#                                  method='transfer', gas_limit=20000,
#                                  gas_price=100,
#                                  utxo=nutxo)
#        print(address)
    distribution_list = [(nuls1to2(address, config['chain_id'], config['prefix']), balance)
                         for address, balance in holders.items()]
    max_items = config.get('bulk_max_items')
    for i in range(math.ceil(len(distribution_list) / max_items)):
        step_items = distribution_list[max_items*i:max_items*(i+1)]
        print([[i[0] for i in step_items],
             [str(int(i[1])) for i in step_items]])
        txhash = await contract_call_packer(
            server, config['source_address'], config['contract_address'],
            'bulkTransfer',
            [[i[0] for i in step_items],
             [str(int(i[1])) for i in step_items]],
            pri_key, remark=config['balances_remark'],
            chain_id=config['chain_id'],
            asset_id=config.get('asset_id', 1),
            gas_limit=len(step_items)*30000)
        print("balances stage", i, len(step_items), "items", txhash)

@click.command()
@click.option('--config', '-c', default='config.yaml', help='Config file')
def main(config):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(rmain(config))

if __name__ == '__main__':
    main()
