from vk_api import VkApi


async def download_posts_incrementally(vk: VkApi, group_domain: str, last_post_count: int):
    posts_raw = {"items": [], "group": group_domain}
    p_tmp = vk.wall.get(domain=group_domain, count=1)
    has_pinned_post = False
    if len(p_tmp["items"]) > 0:
        has_pinned_post = p_tmp["items"][0].get("is_pinned") == 1
    current_count = p_tmp["count"]
    if current_count == last_post_count:
        return posts_raw
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
        posts_raw_tmp = vk.wall.get(domain=group_domain, offset=download_offset, count=to_download)
        posts_raw["items"].extend(posts_raw_tmp["items"])
        last_post_count += len(posts_raw_tmp["items"])
    return posts_raw
