input_file = "data.psql"
output_file = "insert.sql"

table = "storage.objects"

columns = [
    "id","bucket_id","name","owner","created_at","updated_at","last_accessed_at",
    "metadata","version","owner_id","user_metadata","level"
]

rows = []

with open(input_file,"r",encoding="utf-8") as f:
    for line in f:
        line=line.rstrip("\n")
        if line.startswith("COPY "): continue
        if line.strip() == "\\.": continue
        if len(line.strip()) == 0: continue
        parts = line.split("\t")
        
        # unescape json metadata
        parts[7] = parts[7].replace("\\\"","\"").replace("\"{","{").replace("}\"","}")
        
        # quote every field in row except level
        for i in range(len(parts)):
            if i == len(parts)-1:  # level (int)
                continue
            parts[i] = "'" + parts[i].replace("'","''") + "'"
        rows.append("(" + ",".join(parts) + ")")

with open(output_file,"w",encoding="utf-8") as f:
    f.write(f"INSERT INTO {table} ({','.join(columns)}) VALUES\n")
    f.write(",\n".join(rows))
    f.write("\nON CONFLICT (id) DO NOTHING;\n")


print("DONE. insert.sql generated.")
