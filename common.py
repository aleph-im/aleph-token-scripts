
import aiohttp
import time
import struct

from nuls2.model.data import (
    NulsSignature, public_key_to_hash, address_from_hash, hash_from_address,
    CHEAP_UNIT_FEE, b58_decode)
from nuls2.model.transaction import Transaction

# BASE_URL = 'https://nuls.world'

async def get_address(pubkey, chain_id, prefix):
    phash = public_key_to_hash(pubkey, chain_id=chain_id)
    address = address_from_hash(phash, prefix=prefix)
    return address


async def broadcast(server, tx_hex, chain_id=1):
    return await server.broadcastTx(chain_id, tx_hex)

async def get_balance(server, address, chain_id, asset_id):
    return await server.getAccountBalance(chain_id, chain_id,
                                          asset_id, address)
        
async def prepare_transfer_tx(address, targets, nonce, chain_id=1,
                              asset_id=1, remark=""):
    """ Targets are tuples: address and value.
    """
    outputs = [
        {"address": add,
         "amount": val,
         "lockTime": -1,
         "assetsChainId": chain_id,
         "assetsId": asset_id} for add, val in targets
    ]
    # change = sum([inp['value'] for inp in utxo]) - sum([o['value'] for o in outputs])
    # outputs.append({
    #     "address": hash_from_address(address),
    #     "value": change,
    #     "lockTime": 0
    # })
    tx = await Transaction.from_dict({
        "type": 2,
        "time": int(time.time()),
        "remark": remark.encode('utf-8'),
        "coinFroms": [
            {
                'address': address,
                'assetsChainId': chain_id,
                'assetsId': asset_id,
                'amount': 0,
                'nonce': nonce,
                'locked': 0
            }
        ],
        "coinTos": outputs
    })
    tx.inputs[0]['amount'] = (
        (await tx.calculate_fee())
        + sum([o['amount'] for o in outputs]))
    return tx
    
            
async def prepare_businessdata_tx(address, utxo, content):
    tx = await Transaction.from_dict({
      "type": 10,
      "time": int(time.time()),
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
                                   method, args, nonce, value=0,
                                   remark='', chain_id=1, asset_id=1,
                                   method_desc='',
                                   gas_price=25,
                                   gas_limit=2000000):
    tx = await Transaction.from_dict({
        "type": 16,
        "time": int(time.time()),
        "remark": remark.encode('utf-8'),
        "txData": {
            'sender': address,
            'contractAddress': contract_address,
            'value': value,
            'gasLimit': gas_limit,
            'price': gas_price,
            'methodName': method,
            'methodDesc': method_desc, # why is this even needed?
            'args': args
        },
        "coinFroms": [
            {
                'address': address,
                'assetsChainId': chain_id,
                'assetsId': asset_id,
                'amount': (100000) + (gas_price*gas_limit),
                'nonce': nonce,
                'locked': 0
            }
        ],
        "coinTos": []
    })
    tx.inputs[0]['amount'] = (((await tx.calculate_fee())*2)
                              + (gas_price*gas_limit) + value)
    return tx

async def distribution_packer(
        server,
        account_address, contract_address,
        target_addresses, private_key,
        value=10, nonce=None,
        remark='testing',
        method='bulkTransfer', gas_price=25,
        chain_id=1, asset_id=1,
        gas_limit=2000000):
    # print("NULS Connector set up with address %s" % address)
    if nonce is None:
        balance_info = await get_balance(server,
                                         account_address,
                                         chain_id, asset_id)
        nonce = balance_info['nonce']
        
    tx = await prepare_contract_call_tx(account_address, contract_address, method,
                                        [target_addresses, [str(int(value*(10**10)))]*len(target_addresses)],
                                        nonce, remark=remark,
                                        chain_id=chain_id, asset_id=asset_id,
                                        gas_limit=gas_limit, gas_price=gas_price)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    ret = await broadcast(server, tx_hex, chain_id=chain_id)
    
    return ret['hash']


async def contract_call_packer(server, account_address, contract_address,
                               method, params, private_key,
                               nonce=None, remark='',
                               chain_id=1, asset_id=1,
                               gas_price=25,
                               gas_limit=2000000):
    # print("NULS Connector set up with address %s" % address)
    if nonce is None:
        balance_info = await get_balance(server,
                                         account_address,
                                         chain_id, asset_id)
        nonce = balance_info['nonce']
    
    tx = await prepare_contract_call_tx(account_address, contract_address, method,
                                        params, nonce, remark=remark,
                                        chain_id=chain_id, asset_id=asset_id,
                                        gas_limit=gas_limit, gas_price=gas_price)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    # tx_hash = await tx.get_hash()
    # print("Broadcasting TX")
    ret = await broadcast(server, tx_hex, chain_id=chain_id)
    
    return ret['hash']

async def transfer_packer(server, account_address, targets,
                          private_key, nonce=None, remark='',
                          chain_id=1, asset_id=1,):
    # print("NULS Connector set up with address %s" % address)
    if nonce is None:
        balance_info = await get_balance(server,
                                         account_address,
                                         chain_id, asset_id)
        nonce = balance_info['nonce']
        
    tx = await prepare_transfer_tx(account_address, targets, nonce,
                                   chain_id=chain_id, asset_id=asset_id,
                                   remark=remark)
    await tx.sign_tx(private_key)
    tx_hex = (await tx.serialize()).hex()
    # tx_hash = await tx.get_hash()
    # print("Broadcasting TX")
    ret = await broadcast(server, tx_hex, chain_id=chain_id)
    return ret['hash']

async def get_sent_nuls(source_address, db, remark=None):
    matches = {
            'type': 2,
            'coinFroms.address': source_address
        }
    
    if remark is not None:
        matches['remark'] = remark
        
    items = db.transactions.aggregate([
        {'$match': matches},
        {'$unwind': '$coinTos'},
        {'$group': {
            '_id': '$coinTos.address',
            'amount': {
                '$sum': '$coinTos.amount'
            }
        }}
    ])
    return {
        it['_id']: it['amount']
        async for it in items
    }

async def get_sent_tokens(source_address, contract_address, db, remark=None):
    matches = {
        'type': 16,
        'txData.contractAddress': contract_address,
        'txData.resultInfo.tokenTransfers.fromAddress': source_address
    }
    
    if remark is not None:
        matches['remark'] = remark
        
    items = db.transactions.aggregate([
        {'$match': matches},
        {'$unwind': '$txData.resultInfo.tokenTransfers'},
        {'$match': {
            'txData.resultInfo.tokenTransfers.fromAddress': source_address
        }},
        {'$group': {
            '_id': '$txData.resultInfo.tokenTransfers.toAddress',
            'value': {
                '$sum': {'$toDouble': "$txData.resultInfo.tokenTransfers.value"}
            }
        }}
    ])
    return {
        it['_id']: it['value']
        async for it in items
    }
    
def nuls1to2(address, chain_id, prefix):
    addrhash = b58_decode(address)[2:-1]
    addr_type = addrhash[0]
    addrhash = bytes(struct.pack("h", chain_id)) + \
               addrhash
    return address_from_hash(addrhash, prefix=prefix)