import argparse
import asyncio
import queue
import sys
import threading
import time

import dataset
import requests
import toml
import vk_api
from mastodon import Mastodon

import utils

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Config path")
parser.add_argument("-i", "--interval", type=int, help="Polling interval", required=True)
args = parser.parse_args()

if args.interval <= 0:
    print("interval must be greater than 0")
    sys.exit(0)

config = toml.load(args.config)

vk_session = vk_api.VkApi(token=config["vk"]["access_token"])
vk = vk_session.get_api()

mastodon_clients = {}
bot_threads = {}
q = queue.Queue()

db = dataset.connect('sqlite:///database.db')
uploaded_posts = db['uploaded_posts']


def bot_loop():
    while True:
        # get new post from queue
        post_chunk = q.get(block=True, timeout=None)

        m = mastodon_clients.get(post_chunk["group"])
        if m is None:
            print(f"couldn't find corresponding mastodon client for group {post_chunk['group']}")
            continue

        for post in post_chunk["items"]:
            attachments = post.get("attachments")

            parsed_post = {"id": post["id"], "text": post["text"], "date": post["date"],
                           "pinned": post.get("is_pinned") == 1,
                           "attachments": []}
            if attachments is not None:
                for a in attachments:
                    if a["type"] == "photo":
                        # get the biggest resolution of the photo
                        a["photo"]["sizes"].sort(key=lambda e: e["height"], reverse=True)
                        parsed_post["attachments"].append(a["photo"]["sizes"][0]["url"])

            uploaded_media = []
            for i in parsed_post["attachments"]:
                resp = requests.get(i)
                m = m.media_post(resp.content, mime_type='image/jpeg')
                uploaded_media.append(m)
            toot = m.status_post(parsed_post["text"], media_ids=uploaded_media, visibility='public')
            if parsed_post['pinned']:
                m.status_pin(toot['id'])
            uploaded_posts.insert({'group': post_chunk["group"], 'post_id': post['id']})

            group_last_post_count = db['last_post_count'].find_one(group=post_chunk["group"])
            if group_last_post_count is None:
                group_last_post_count = {'count': 0, 'group': post_chunk["group"]}  # FIXME this shouldn't happen
            group_last_post_count['count'] += 1
            db['last_post_count'].upsert(group_last_post_count, ['group'])
            print(f"Uploaded post {post['id']} for group {post_chunk['group']} successfully!")


async def listen_new_posts():
    tasks = []
    for group in config["mastodon"]:
        group_last_post_count = db['last_post_count'].find_one(group=group)
        if group_last_post_count is None:
            group_last_post_count = {'count': 0,
                                     'group': group}  # FIXME need to execute archive.py code for full downloading of wall on new group
            # FIXME this shouldn't happen
        tasks.append(utils.download_posts_incrementally(vk, group, group_last_post_count['count']))
    new_post_chunks = await asyncio.gather(*tasks)
    for chunk in new_post_chunks:
        if len(chunk["items"]) > 0:
            q.put(chunk, block=True, timeout=None)

for k in config["mastodon"]:
    mastodon_clients[k] = Mastodon(
        access_token=config["mastodon"][k]["access_token"],
        api_base_url=config["mastodon"][k]["instance"]
    )
    print(k)

    t = threading.Thread(target=bot_loop)
    t.start()
    bot_threads[k] = t

print("Bot has been set up, listening events...")


while True:
    try:
        asyncio.run(listen_new_posts())
        time.sleep(args.interval)
    except KeyboardInterrupt:
        db.close()
        break

