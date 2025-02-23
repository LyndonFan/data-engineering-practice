import csv
import json
from pathlib import Path

def main(folder: Path, destination_file: Path):
    rows = []
    for file in folder.rglob("*.json"):
        with open(file, "r") as f:
            rows.append(json.load(f))
    for row in rows:
        row["geolocation_type"] = row["geolocation"]["type"]
        row["geolocation_coordinates_0"] = row["geolocation"]["coordinates"][0]
        row["geolocation_coordinates_1"] = row["geolocation"]["coordinates"][1]
        row.pop("geolocation")
    with open(destination_file, "w+", newline="") as fo:
        writer = csv.DictWriter(fo, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
        


if __name__ == "__main__":
    file_parent = Path().parent
    main(file_parent / "data", file_parent / "result.csv")
