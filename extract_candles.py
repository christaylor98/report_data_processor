import json

input_file = "misc/dash_part.jsln"
output_file = "candles_only.jsln"

with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
    for line in infile:
        line = line.strip()
        if line and '"t":"c"' in line:
            outfile.write(line + '\n')

print(f"Candle data extracted to {output_file}") 