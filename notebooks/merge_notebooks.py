import nbformat
from nbformat.v4 import new_markdown_cell

files = [
    "01_Data_Processing_and_Analysis.ipynb",
    "02_Machine_Learning_Models.ipynb",
    "03_RealTime_Streaming_Process.ipynb"
]

print("Se încarcă notebook-urile...")

merged = nbformat.read(files[0], as_version=4)

for f in files[1:]:
    print(f"Adaug: {f}")

    merged.cells.append(
        new_markdown_cell("---")
    )

    nb = nbformat.read(f, as_version=4)
    merged.cells.extend(nb.cells)

output = "AirTrafficAnalytics.ipynb"

nbformat.write(merged, output)

print(f"\n✔ Notebook creat cu succes: {output}")