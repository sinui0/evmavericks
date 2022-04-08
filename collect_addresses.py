import pandas as pd
import asyncpraw
import os
import re
import asyncio
from dotenv import load_dotenv
load_dotenv()

address_pattern = re.compile(r'(0x[a-fA-F0-9]{40})')
def extract_address(t):
    addresses = address_pattern.search(t)
    if addresses:
        return addresses.group(0)

files = os.listdir('.')
if 'all_addresses.csv' not in files:
    df = pd.DataFrame(columns=['author','address'])
    df.to_csv('all_addresses.csv', index=False)

whitelist = set(pd.read_csv('whitelist.csv')['author'].tolist())

async def run():
    reddit = asyncpraw.Reddit(
        client_id=os.environ['CLIENT_ID'],
        client_secret=os.environ['CLIENT_SECRET'],
        username=os.environ['USER_NAME'],
        password=os.environ['PASSWORD'],
        user_agent="python:ethmavericks:v0.1.0 (by u/Bad_Investment)"
    )

    addrs = []
    async for msg in reddit.inbox.messages():
        if msg.author and isinstance(msg, asyncpraw.models.Message):
            addr = extract_address(msg.body)
            if addr and (msg.author.name in whitelist):
                addrs.append({'author':msg.author.name, 'address':addr})
        await msg.mark_read()

    if addrs:
        df = pd.DataFrame(addrs).drop_duplicates('author')
        df_existing = pd.read_csv('all_addresses.csv')

        # Save new addresses
        df_new = df[~df['author'].isin(df_existing['author'])]
        df_new.to_csv('new_addresses.csv', index=False)

        # Save addresses to full record so we don't count them twice
        df = pd.concat([df, df_existing]).drop_duplicates('author')
        df.to_csv('all_addresses.csv', index=False)

    await reddit.close()

if __name__ == '__main__':
    asyncio.run(run())