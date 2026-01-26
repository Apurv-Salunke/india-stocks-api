#!/usr/bin/env python3
import os
import sys
import yaml
import shutil
import re
import argparse
import subprocess
from pathlib import Path

def load_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def run_command(cmd, cwd=None):
    result = subprocess.run(cmd, shell=True, cwd=cwd, text=True, capture_output=True)
    if result.returncode != 0:
        print(f"Error executing: {cmd}")
        print(result.stderr)
        sys.exit(1)
    return result.stdout.strip()

def apply_rules(content, rules):
    for rule in rules:
        pattern = rule['pattern']
        replacement = rule['replace']
        # Simple string replacement if no regex chars, otherwise regex
        # For module imports, simple regex is safer to avoid partial matches if needed
        # But per specs, simple replacement might suffice. 
        # Using simple replacement for now as patterns are specific imports
        if pattern in content:
            content = content.replace(pattern, replacement)
    return content

def sync_broker(broker_name, config, source_root, dest_root, rules):
    source_dir = Path(source_root) / config['source']
    dest_dir = Path(dest_root) / config['dest']
    
    print(f"Syncing {broker_name}...")
    print(f"  Source: {source_dir}")
    print(f"  Dest:   {dest_dir}")

    files_processed = 0
    
    # helper
    def process_file(rel_path, is_shimmed=False):
        src = source_dir / rel_path
        dst = dest_dir / rel_path
        
        if not src.exists():
            print(f"  ⚠️  Source missing: {rel_path}")
            return

        dst.parent.mkdir(parents=True, exist_ok=True)
        
        with open(src, 'r') as f:
            content = f.read()
            
        if is_shimmed:
            content = apply_rules(content, rules)
            
        with open(dst, 'w') as f:
            f.write(content)
            
        print(f"  ✅ Synced: {rel_path} ({'Shimmed' if is_shimmed else 'Copy'})")

    # Process Shimmed
    for f in config['files'].get('shimmed', []):
        process_file(f, is_shimmed=True)
        files_processed += 1
        
    # Process Passthrough
    for f in config['files'].get('passthrough', []):
        process_file(f, is_shimmed=False)
        files_processed += 1
        
    print(f"  ✨ Synced {files_processed} files for {broker_name}")

def main():
    parser = argparse.ArgumentParser(description="Sync upstream broker code.")
    parser.add_argument("--source", required=True, help="Path to OpenAlgo source root")
    parser.add_argument("--dest", default=".", help="Project root (default: current dir)")
    parser.add_argument("--manifest", default="scripts/broker_manifest.yaml", help="Path to manifest")
    parser.add_argument("--rules", default="scripts/shim_rules.yaml", help="Path to shim rules")
    
    args = parser.parse_args()
    
    manifest = load_yaml(args.manifest)
    rules = load_yaml(args.rules)['rules']
    
    for broker_name, config in manifest['brokers'].items():
        sync_broker(broker_name, config, args.source, args.dest, rules)

if __name__ == "__main__":
    main()
