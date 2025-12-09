import json

notebook_path = r'notebooks/cab_wise_analysis.ipynb'

with open(notebook_path, encoding='utf-8') as f:
    nb = json.load(f)

# Fix cell 12 (FHVHV data section)
cell_source = nb['cells'][12]['source']

# Find and replace the problematic aggregation
new_lines = []
i = 0
while i < len(cell_source):
    line = cell_source[i]
    if 'hourly_fhvhv = fhvhv_data.groupby' in line:
        # Replace the agg section
        new_lines.append("    # Demand by hour\n")
        new_lines.append("    hourly_fhvhv = fhvhv_data.groupby('hour').agg(\n")
        new_lines.append("        trips=('trip_miles', 'count'),\n")
        new_lines.append("        avg_fare=('base_passenger_fare', 'mean'),\n")
        new_lines.append("        total_revenue=('base_passenger_fare', 'sum'),\n")
        new_lines.append("        avg_tips=('tips', 'mean')\n")
        new_lines.append("    ).reset_index()\n")
        
        # Skip old lines until reset_index
        while i < len(cell_source) and '.reset_index()' not in cell_source[i]:
            i += 1
        i += 1  # Include the .reset_index() line
        
        # Skip the columns assignment line
        if i < len(cell_source) and 'hourly_fhvhv.columns' in cell_source[i]:
            i += 1
    else:
        new_lines.append(line)
        i += 1

nb['cells'][12]['source'] = new_lines

with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print("Fixed FHVHV hourly aggregation")
