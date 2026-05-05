import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

def run_script(script_name):
    print(f"\n{'='*50}\nExecuting {script_name}...\n{'='*50}")
    script_path = BASE_DIR / script_name
    try:
        subprocess.run([sys.executable, str(script_path)], check=True)
        print(f"✓ {script_name} completed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"✗ Error executing {script_name}. Process exited with code {e.returncode}. Aborting pipeline.")
        sys.exit(1)
    except FileNotFoundError:
        print(f"✗ Script {script_name} not found. Aborting.")
        sys.exit(1)

def main():
    print("==================================================")
    print("Starting Fully Reproducible Data Pipeline")
    print("This will process 8M+ patents in chunks for an 8GB laptop.")
    print("==================================================\n")
    
    # 1. Setup DB Schema
    run_script("create_db.py")
    
    # 2. Extract Data from TSV to memory-efficient chunks
    run_script("extract_data.py")
    
    # 3. Clean and normalize data efficiently
    run_script("clean_data.py")
    
    # 4. Load batched clean CSVs into MySQL using SQLAlchemy
    run_script("load_data.py")
    
    # 5. Generate all JSON and CSV reports (uses ML models & SQLAlchemy)
    run_script("export_results.py")
    
    print("\n==================================================")
    print("Pipeline Complete! All data processed, loaded, and reports generated.")
    print("You can now run the dashboard via:")
    print("  streamlit run dashboard.py")
    print("==================================================")

if __name__ == "__main__":
    main()
