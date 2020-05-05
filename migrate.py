import asyncio
import json
import time
import math
import yaml
import click
from secp256k1 import PrivateKey

from common import contract_call_packer, broadcast, get_address
from nuls2.api.server import get_server
from aleph_client.asynchronous import fetch_aggregate

async def rmain(config_file, remark):
    with open(config_file, 'r') as stream:
        config = yaml.safe_load(stream)
        
    pri_key = bytes.fromhex(config['distribution_pkey'])
    # address = await get_address(pub_key, config['chain_id'], config['prefix'])
    # pri_key = bytes.fromhex(config['source_pkey'])
    privkey = PrivateKey(pri_key, raw=True)
    pub_key = privkey.pubkey.serialize()
    address = await get_address(pub_key, config['chain_id'], config['prefix'])
    server = get_server(config['api_server'])
    
    distribution_list = []    
    content = await fetch_aggregate(config['aleph_balances_oracle'], 'contract_'+config['old_contract_address'] , api_server=config['aleph_api_server'])
    distribution_list = [
        (address, amount)
        for address, amount in content['holders'].items()
        if address != config['old_source_address'] 
    ]
    print(distribution_list)

    nonce = None

    # return
#    for address in addresses[:50]:
#        nutxo = await nuls_packer([address],
#                                  method='transfer', gas_limit=20000,
#                                  gas_price=100,
#                                  utxo=nutxo)
#        print(address)
    max_items = config.get('bulk_max_items')
    if len(distribution_list):
        for i in range(math.ceil(len(distribution_list) / max_items)):
            step_items = distribution_list[max_items*i:max_items*(i+1)]
            nash = await contract_call_packer(
                server, config['distribution_address'], config['contract_address'],
                'batchTransferFrom',
                [[config['source_address'],],
                 [i[0] for i in step_items],
                 [str(int(i[1])) for i in step_items]],
                pri_key, nonce=nonce, remark=remark,
                chain_id=config['chain_id'],
                asset_id=config.get('asset_id', 1),
                gas_limit=len(step_items)*30000)
            nonce = nash[-16:]
            await asyncio.sleep(10)
            print("distribution stage", i, len(step_items), "items")
            
@click.command()
@click.option('--config', '-c', default='config.yaml', help='Config file')
@click.option('--remark', '-r', default='ALEPH-BALANCES', help='Remark')
def main(config, remark):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(rmain(config, remark))

if __name__ == '__main__':
    main()

