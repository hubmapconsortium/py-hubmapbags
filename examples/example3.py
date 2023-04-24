import hubmapbags

hubmap_id = "HBM666.FFFW.363"
token = "<this-is-my-token>"
instance = "prod"  # default instance is test

uuids = hubmapbags.uuids.get_uuids(hubmap_id, instance=instance, token=token)

print(f"UUIDs for dataset {hubmap_id}")
print(uuids)
