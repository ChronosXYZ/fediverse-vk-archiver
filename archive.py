import argparse
import sys

import dataset
import requests
import toml
import vk_api
from mastodon import Mastodon

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

print(f"Downloading list of posts in group {args.group}...")
parsed_posts = []

db = dataset.connect('sqlite:///database.db')
last_post_count_table = db['last_post_count']
uploaded_posts = db['uploaded_posts']

group_last_post_count = last_post_count_table.find_one(group=args.group)

posts_raw = {}
if group_last_post_count == None:
    # download full wall
    posts_raw = tools.get_all('wall.get', 100, {'domain': args.group})
else:
    # download only neccessary posts from vk
    posts_raw["items"] = []
    last_post_count = group_last_post_count["count"]
    p_tmp = vk.wall.get(domain=args.group, count=1)
    has_pinned_post = p_tmp["items"][0].get("is_pinned") == 1
    current_count = p_tmp["count"]
    if current_count == last_post_count:
        print("Nothing to do, quitting...")
        sys.exit(0)
    posts_raw["count"] = current_count
    download_count = current_count - last_post_count
    download_offset = 0
    if has_pinned_post:
        # skip pinned post, cuz it appears first in the list
        download_offset += 1
    while download_count > 0:
        to_download = 0
        if download_count - 100 < 0:
            to_download = download_count
            download_count = 0
        else:
            to_download = 100
            download_count -= 100
            download_offset += 100
        posts_raw_tmp = vk.wall.get(domain=args.group, offset=download_offset, count=to_download)
        posts_raw["items"].extend(posts_raw_tmp["items"])
        last_post_count += len(posts_raw_tmp["items"])
posts = posts_raw["items"]
for p in posts:
    attachments = p.get("attachments")
    if attachments == None:
        continue
    parsed_post = {}
    parsed_post["id"] = p["id"]
    parsed_post["text"] = p["text"]
    parsed_post["date"] = p["date"]
    parsed_post["pinned"] = p.get("is_pinned") == 1
    parsed_post["attachments"] = []
    for a in attachments:
        if a["type"] == "photo":
            # get the biggest resolution of the photo
            a["photo"]["sizes"].sort(key=lambda e: e["height"], reverse=True)
            parsed_post["attachments"].append(a["photo"]["sizes"][0]["url"])
    parsed_posts.append(parsed_post)

parsed_posts.sort(key=lambda e: e["date"])

print("Uploading posts to the Fediverse...")
if group_last_post_count == None:
    group_last_post_count = {}
    group_last_post_count['count'] = 0
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
