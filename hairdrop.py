import asyncio
import json
import time
import math
import yaml
from secp256k1 import PrivateKey

from common import BASE_URL, get_utxo, prepare_contract_call_tx, broadcast, get_address, distribution_packer



    


async def main():
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
        
    pri_key = bytes.fromhex(config['airdrop_pkey'])
    privkey = PrivateKey(pri_key, raw=True)
    pub_key = privkey.pubkey.serialize()
    address = await get_address(pub_key, config['network_id'])
    print(address)

    addresses = []
    for line in open('/home/jon/Documents/Aleph/snapshots/cached_unspent-testnet-20190527.json', 'r'):
        val = json.loads(line)
        if val['_id'].startswith('TT'):
            unspent = val['unspent_value']
            if isinstance(unspent, dict):
                unspent = int(unspent['$numberLong'])
            
            if unspent >= 99500000:
                addresses.append(val['_id'])

    nutxo = None
#    for address in addresses[:50]:
#        nutxo = await nuls_packer([address],
#                                  method='transfer', gas_limit=20000,
#                                  gas_price=100,
#                                  utxo=nutxo)
#        print(address)
    MAX_ITEMS = 120
    for i in range(math.ceil(len(addresses) / MAX_ITEMS)):
        nutxo = await distribution_packer(address, config['contract_address'],
                                          addresses[MAX_ITEMS*i:MAX_ITEMS*(i+1)],
                                          pri_key,
                                          utxo=nutxo, remark="ALT2 Airdrop 1b",
                                          value=100)
        print(i, len(addresses[MAX_ITEMS*i:MAX_ITEMS*(i+1)]))


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
