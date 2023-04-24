import hubmapbags

id = "HBM666.FFFW.363"
dbgap_study_id = "phs002267"
token = "<this-is-my-token>"

answer = hubmapbags.uuids.populate_local_file_with_remote_uuids(
    id, instance="prod", token=token, debug=True
)

print(f"UUIDs for dataset {hubmap_id}")
print(answer)
