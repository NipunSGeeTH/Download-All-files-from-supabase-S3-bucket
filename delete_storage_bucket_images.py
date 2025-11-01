from supabase import create_client, Client

SERVICE_ROLE_KEY = ""   # EDIT
PROJECT_REF     = ""         # EDIT

bucket_name = "questions"

# read keep list from text file
with open("keepimageslist.txt", "r") as f:
    keep_files = [x.strip() for x in f.readlines() if x.strip() != ""]

url = f"https://{PROJECT_REF}.supabase.co"
supabase: Client = create_client(url, SERVICE_ROLE_KEY)

all_files = supabase.storage.from_(bucket_name).list()

delete_files = []

for f in all_files:
    if f["name"] not in keep_files:
        delete_files.append(f["name"])

print("Will delete:", delete_files)

if delete_files:
    supabase.storage.from_(bucket_name).remove(delete_files)
    print("DELETE DONE")
else:
    print("NOTHING TO DELETE")
