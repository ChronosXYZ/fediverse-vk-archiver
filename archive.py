import argparse
import asyncio
import sys

import dataset
import requests
import toml
import vk_api
from mastodon import Mastodon

import utils

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", help="Config path")
parser.add_argument("-g", "--group", help="VK group to archive")
args = parser.parse_args()

config = toml.load(args.config)

if config["mastodon"].get(args.group) is None:
    print("invalid group")
    sys.exit(1)

mastodon = Mastodon(
    access_token=config["mastodon"][args.group]["access_token"],
    api_base_url=config["mastodon"][args.group]["instance"]
)

vk_session = vk_api.VkApi(token=config["vk"]["access_token"])
vk = vk_session.get_api()

tools = vk_api.VkTools(vk_session)

parsed_posts = []

db = dataset.connect('sqlite:///database.db')
last_post_count_table = db['last_post_count']
uploaded_posts = db['uploaded_posts']

group_last_post_count = last_post_count_table.find_one(group=args.group)

posts_raw = {}
print(f"Downloading list of posts in group {args.group}...")
if group_last_post_count is None:
    # download full wall
    posts_raw = tools.get_all('wall.get', 100, {'domain': args.group})
else:
    # download only necessary posts from vk
    last_post_count = group_last_post_count["count"]
    posts_raw["items"] = asyncio.run(utils.download_posts_incrementally(vk, args.group, last_post_count))
posts = posts_raw["items"]
for p in posts:
    if uploaded_posts.find_one(group=args.group, post_id=p["id"]) is not None:
        print(f"Post {p['id']} already has been uploaded, skipping it...")
        continue

    attachments = p.get("attachments")
    parsed_post = {"id": p["id"], "text": p["text"], "date": p["date"], "pinned": p.get("is_pinned") == 1,
                   "attachments": []}
    if attachments is not None:
        for a in attachments:
            if a["type"] == "photo":
                # get the biggest resolution of the photo
                a["photo"]["sizes"].sort(key=lambda e: e["height"], reverse=True)
                parsed_post["attachments"].append(a["photo"]["sizes"][0]["url"])
    parsed_posts.append(parsed_post)

parsed_posts.sort(key=lambda e: e["date"])

print("Uploading posts to the Fediverse...")
if group_last_post_count is None:
    group_last_post_count = {'count': 0, 'group': args.group}
c = 0
for p in parsed_posts:
    uploaded_media = []
    for i in p["attachments"]:
        resp = requests.get(i)
        m = mastodon.media_post(resp.content, mime_type='image/jpeg')
        uploaded_media.append(m)
    toot = mastodon.status_post(p["text"], media_ids=uploaded_media, visibility='public')
    if p['pinned']:
        mastodon.status_pin(toot['id'])
    uploaded_posts.insert({'group': args.group, 'post_id': p['id']})
    group_last_post_count['count'] += 1
    last_post_count_table.upsert(group_last_post_count, ['group'])
    c += 1
    print(f"Progress: {c}/{len(parsed_posts)}")

db.close()
