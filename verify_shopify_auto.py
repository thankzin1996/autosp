
import importlib.util
import sys
import os
import json

file_path = r"c:\Users\pc\Downloads\autosp\shopify_auto main.py"

spec = importlib.util.spec_from_file_location("shopify_auto_main", file_path)
shopify_auto_main = importlib.util.module_from_spec(spec)
sys.modules["shopify_auto_main"] = shopify_auto_main
spec.loader.exec_module(shopify_auto_main)

cc = "5509890034877216|06|2028|333"
site = "eternal-tattoo-supply.myshopify.com"
proxy = None

print(f"Testing process_checkout with site: {site}")

try:
    result = shopify_auto_main.process_checkout(cc, site, proxy)
    print("\n--- Result ---")
    print(json.dumps(result, indent=2))
except Exception as e:
    print(f"\n--- Error ---")
    print(str(e))
    import traceback
    traceback.print_exc()
