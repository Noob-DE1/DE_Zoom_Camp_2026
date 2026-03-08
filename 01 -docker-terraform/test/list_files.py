from pathlib import Path

courrent_dir = Path.cwd()

current_file = Path(__file__).name

print(f"filles in {courrent_dir}:")

for filepath in courrent_dir.iterdir():
    if filepath.name == current_file:
        continue
    
    print(f"   -{filepath.name}")

    if filepath.is_file():

        content = filepath.read_text(encoding='utf-8')
        print(f"    Content: {content}")
        