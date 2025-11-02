import csv

infile = "data.psql"     # your input file which contains COPY ... and rows
outfile = "objects.csv"      # output csv for import

with open(infile, "r") as f:
    lines = f.read().splitlines()

data_lines = []
for line in lines:
    if line.startswith("COPY "): continue
    if line.strip() == "\.": continue
    if line.strip() == "": continue
    data_lines.append(line)

rows = [x.split("\t") for x in data_lines]

header = ["id","bucket_id","name","owner","created_at","updated_at","last_accessed_at","metadata","version","owner_id","user_metadata","level"]

with open(outfile, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(rows)

print("DONE → CSV created:", outfile)
