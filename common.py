
import aiohttp
import time

from nulsexplorer.protocol.data import (
    NulsSignature, public_key_to_hash, address_from_hash, hash_from_address,
    CHEAP_UNIT_FEE)
from nulsexplorer.protocol.transaction import Transaction

BASE_URL = 'https://testnet.nuls.world'

async def get_address(pubkey, chain_id):
    phash = public_key_to_hash(pubkey, chain_id=chain_id)
    address = address_from_hash(phash)
    return address


async def broadcast(tx_hex):
    broadcast_url = '{}/broadcast'.format(BASE_URL)
    data = {'txHex': tx_hex}

    async with aiohttp.ClientSession() as session:
        async with session.post(broadcast_url, json=data) as resp:
            resp2 = await resp.json()
            if resp2 is None:
                raise ValueError("No reply on broadcast")
            jres = resp2['value']
            return jres

async def get_utxo(address):
    check_url = '{}/addresses/outputs/{}.json'.format(BASE_URL, address)

    async with aiohttp.ClientSession() as session:
        async with session.get(check_url) as resp:
            jres = await resp.json()
            return jres['outputs']
        
async def prepare_transfer_tx(address, targets, utxo, remark=""):
    """ Targets are tuples: address and value.
    """
    print(targets)
    outputs = [
        {"address": hash_from_address(add),
         "value": val,
         "lockTime": 0} for add, val in targets
    ]
    change = sum([inp['value'] for inp in utxo]) - sum([o['value'] for o in outputs])
    outputs.append({
        "address": hash_from_address(address),
        "value": change,
        "lockTime": 0
    })
    tx = await Transaction.from_dict({
      "type": 2,
      "time": int(time.time() * 1000),
      "blockHeight": None,
      "fee": 0,
      "remark": remark.encode('utf-8'),
      "scriptSig": b"",
      "inputs": [{'fromHash': inp['hash'],
                  'fromIndex': inp['idx'],
                  'value': inp['value'],
                  'lockTime': inp['lockTime']} for inp in utxo],
      "outputs": outputs
    })
    tx.coin_data.outputs[-1].na = (
        sum([inp['value'] for inp in utxo])
        - (await tx.calculate_fee()))
    return tx
    
            
async def prepare_businessdata_tx(address, utxo, content):
    tx = await Transaction.from_dict({
      "type": 10,
      "time": int(time.time() * 1000),
      "blockHeight": None,
      "fee": 0,
      "remark": b"",
      "scriptSig": b"",
      "info": {
          "logicData": content.hex()
      },
      "inputs": [{'fromHash': inp['hash'],
                  'fromIndex': inp['idx'],
                  'value': inp['value'],
                  'lockTime': inp['lockTime']} for inp in utxo],
      "outputs": [
          {"address": hash_from_address(address),
           "value": sum([inp['value'] for inp in utxo]),
           "lockTime": 0}
      ]
    })
    tx.coin_data.outputs[0].na = (
        sum([inp['value'] for inp in utxo])
        - (await tx.calculate_fee()))
    return tx

async def prepare_contract_call_tx(address, contract_address,
                                   method, args, utxo, value=0,
                                   remark='',
                                   method_desc='',
                                   gas_price=25,
                                   gas_limit=2000000):
    tx = await Transaction.from_dict({
      "type": 101,
      "time": int(time.time() * 1000),
      "blockHeight": None,
      "fee": 0,
      "remark": remark.encode('utf-8'),
      "scriptSig": b"",
      "info": {
        'sender': address,
        'contractAddress': contract_address,
        'value': value,
        'gasLimit': gas_limit,
        'price': gas_price,
        'methodName': method,
        'methodDesc': method_desc, # why is this even needed?
        'args': args
      },
      "inputs": [{'fromHash': inp['hash'],
                  'fromIndex': inp['idx'],
                  'value': inp['value'],
                  'lockTime': inp['lockTime']} for inp in utxo],
      "outputs": [
          {"address": hash_from_address(address),
           "value": sum([inp['value'] for inp in utxo]),
           "lockTime": 0}
      ]
    })
    tx.coin_data.outputs[0].na = (
        sum([inp['value'] for inp in utxo])
        - (await tx.calculate_fee()) - (gas_price*gas_limit) - value)
    return tx

async def distribution_packer(
        account_address, contract_address,
        target_addresses, private_key,
        value=10, utxo=None,
        remark='testing',
        method='bulkTransfer', gas_price=25,
        gas_limit=2000000):
    # print("NULS Connector set up with address %s" % address)
    if utxo is None:
        utxo = await get_utxo(account_address)
    # we take the first 50, hoping it's enough... bad, bad, bad!
    # TODO: do a real utxo management here
    selected_utxo = utxo[:500]
    i = 0
    tx = await prepare_contract_call_tx(account_address, contract_address, method,
                                        [target_addresses, [str(int(value*(10**10)))]*len(target_addresses)],
                                        selected_utxo, remark=remark,
                                        gas_limit=gas_limit, gas_price=gas_price)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    # tx_hash = await tx.get_hash()
    # print("Broadcasting TX")
    tx_hash = await broadcast(tx_hex)
    utxo = [{
        'hash': tx_hash,
        'idx': 0,
        'lockTime': 0,
        'value': tx.coin_data.outputs[0].na
    }]
    return utxo


async def contract_call_packer(account_address, contract_address,
                               method, params, private_key,
                               utxo=None, remark='',
                               gas_price=25,
                               gas_limit=2000000):
    # print("NULS Connector set up with address %s" % address)
    if utxo is None:
        utxo = await get_utxo(account_address)
        
    # we take the first 500, hoping it's enough... bad, bad, bad!
    # TODO: do a real utxo management here
    selected_utxo = utxo[:500]
    i = 0
    tx = await prepare_contract_call_tx(account_address, contract_address, method,
                                        params, selected_utxo, remark=remark,
                                        gas_limit=gas_limit, gas_price=gas_price)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    # tx_hash = await tx.get_hash()
    # print("Broadcasting TX")
    tx_hash = await broadcast(tx_hex)
    utxo = [{
        'hash': tx_hash,
        'idx': 0,
        'lockTime': 0,
        'value': tx.coin_data.outputs[0].na
    }]
    return utxo

async def transfer_packer(account_address, targets,
                          private_key, utxo=None, remark=''):
    # print("NULS Connector set up with address %s" % address)
    if utxo is None:
        utxo = await get_utxo(account_address)
        
    # we take the first 500, hoping it's enough... bad, bad, bad!
    # TODO: do a real utxo management here
    selected_utxo = utxo[:500]
    i = 0
    tx = await prepare_transfer_tx(account_address, targets, selected_utxo, remark=remark)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    # tx_hash = await tx.get_hash()
    # print("Broadcasting TX")
    tx_hash = await broadcast(tx_hex)
    utxo = [{
        'hash': tx_hash,
        'idx': len(tx.coin_data.outputs)-1,
        'lockTime': 0,
        'value': tx.coin_data.outputs[-1].na
    }]
    return utxo

async def get_sent_nuls(source_address, db, remark=None):
    matches = {
            'type': 2,
            'inputs.address': source_address
        }
    
    if remark is not None:
        matches['remark'] = remark
        
    items = db.transactions.aggregate([
        {'$match': matches},
        {'$unwind': '$outputs'},
        {'$group': {
            '_id': '$outputs.address',
            'value': {
                '$sum': '$outputs.value'
            }
        }}
    ])
    return {
        it['_id']: it['value']
        async for it in items
    }

async def get_sent_tokens(source_address, contract_address, db, remark=None):
    matches = {
        'type': 101,
        'info.contractAddress': contract_address,
        'info.result.tokenTransfers.from': source_address
    }
    
    if remark is not None:
        matches['remark'] = remark
        
    items = db.transactions.aggregate([
        {'$match': matches},
        {'$unwind': '$info.result.tokenTransfers'},
        {'$match': {
            'info.result.tokenTransfers.from': source_address
        }},
        {'$group': {
            '_id': '$info.result.tokenTransfers.to',
            'value': {
                '$sum': '$info.result.tokenTransfers.value'
            }
        }}
    ])
    return {
        it['_id']: it['value']
        async for it in items
    }