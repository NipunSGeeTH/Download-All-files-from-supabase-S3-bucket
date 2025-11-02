from supabase import create_client, Client

SERVICE_ROLE_KEY = ""   # EDIT
PROJECT_REF     = ""         # EDIT

bucket_name = "papers"

# read keep list from text file
with open("keepimageslist.txt", "r") as f:
    keep_files = [x.strip() for x in f.readlines() if x.strip() != ""]

url = f"https://{PROJECT_REF}.supabase.co"
supabase: Client = create_client(url, SERVICE_ROLE_KEY)

all_files = supabase.storage.from_(bucket_name).list()

to_delete = [f["name"] for f in all_files if f["name"] not in keep_files]

print("WILL DELETE COUNT:", len(to_delete))

if to_delete:
    # supabase recommend chunking for large bulk delete
    for i in range(0, len(to_delete), 1000):
        chunk = to_delete[i:i+1000]
        supabase.storage.from_(bucket_name).remove(chunk)
    print("DELETE DONE")
else:
    print("NOTHING TO DELETE")