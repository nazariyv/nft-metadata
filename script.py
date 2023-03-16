#!/usr/bin/env python
import requests
import time
from functools import partial
from typing import List, Any
from dotenv import load_dotenv
import os

# ! you have to implement this
from your_db import insert, get_largest_id

load_dotenv()

COLLECTIONS = {
    'BAYC': '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d',
    'MAYC': '0x60e4d786628fea6478f785a6d7e704777c86a7c6',
    'BAKC': '0xba30e5f9bb24caa003e9f2f0497ad287fdf95623',
    'Otherdeed': '0x34d85c9cdeb23fa97cb08333b511ac86e1c4e258',
    'Azuki': '0xed5af388653567af2f388e6224dc7c4b3241c544',
    'CloneX': '0x49cf6f5d44e70224e2e23fdcdd2c053f30ada28b',
    'Doodles': '0x8a90cab2b38dba80c64b7734e58ee1db38b8992e',
    'Milady': '0x5af0d9827e0c53e4799bb226655a1de152a425a5',
    'RumbleKongs': '0xef0182dc0574cd5874494a120750fd222fdb909a',
    'Pudgy': '0xbd3531da5cf5857e7cfaa92426877b022e612cf8'
}

uri = lambda collection_address, start_token, limit: \
      (f'https://eth-mainnet.g.alchemy.com/nft/v2/{os.getenv("ALCHEMY_KEY")}/'
       f'getNFTsForCollection?contractAddress={collection_address}&'
       f'startToken={start_token}&limit={limit}&withMetadata=true')


Meta = List[Any]
def download_all_meta(collection_name: str) -> Meta:
    collection_address = COLLECTIONS[collection_name]
    results = []
    limit = 100
    next_token = get_largest_id(collection_address) + 1
    keep_fetching = True

    while keep_fetching:

        print(f'fetching {collection_name} from {next_token}...')
        req = partial(requests.get, uri(collection_address, next_token, limit))
        r = req()
        while r.status_code != 200:
            time.sleep(5)
            r = req()

        rjson = r.json()
        nfts = rjson['nfts']
        for nft in nfts:
            nft['id']['tokenId'] = int(nft['id']['tokenId'], 16)
        results.extend(nfts)

        if 'nextToken' not in rjson:
            keep_fetching = False
            break
        # converting the id from hex to int
        next_token = int(rjson['nextToken'], 16)

        time.sleep(1)

    return results

def store_all_meta(meta: Meta, collection_address: str) -> bool:
    try:
        insert(meta, collection_address)
    except Exception:
        return False
    return True


if __name__ == '__main__':
    for k in COLLECTIONS:
        collection_name = k
        meta = download_all_meta(collection_name)
        store_all_meta(meta, COLLECTIONS[collection_name])

