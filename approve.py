import asyncio
import json
import time
import math
import yaml

from common import get_address, contract_call_packer
from secp256k1 import PrivateKey

async def main():
    with open("config.yaml", 'r') as stream:
        config = yaml.safe_load(stream)
        
    pri_key = bytes.fromhex(config['source_pkey'])
    privkey = PrivateKey(pri_key, raw=True)
    pub_key = privkey.pubkey.serialize()
    address = await get_address(pub_key, config['network_id'])
    print(address)
    
    nutxo = await contract_call_packer(address, config['contract_address'],
                                        'increaseApproval', 
                                        [[config['distribution_address'],],
                                         [str(1000000*(10**10)),]],
                                        pri_key)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
