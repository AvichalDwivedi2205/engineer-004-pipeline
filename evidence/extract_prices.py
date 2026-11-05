#!/usr/bin/env python3
"""Pull every sku cited in prices_2026-10-29.md out of the raw AWS Price List files.

    # download raw offer files (public, no auth), e.g.
    $ curl -sO https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonKinesis/current/us-east-1/index.json
    $ python3 evidence/extract_prices.py <dir-with-OfferCode.json-files>

Writes evidence/price_list_extract.json: for each sku, the offer file, its
publicationDate, product attributes and on-demand price dimensions, so the
cost sheet's [Benchmarked] inputs can be checked without the 480 MB EC2 file.
"""
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
skus = sorted(set(re.findall(r"\b[A-Z0-9]{16}\b", (HERE / "prices_2026-10-29.md").read_text())))
raw_dir = Path(sys.argv[1])
found = {}
for f in sorted(raw_dir.glob("*.json")):
    if f.name == "index.json":
        continue
    d = json.loads(f.read_text())
    for sku in skus:
        if sku in d.get("products", {}):
            found[sku] = {
                "offer_file": f.name,
                "publication_date": d.get("publicationDate"),
                "source_url": f"https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{d.get('offerCode')}/current/us-east-1/index.json",
                "attributes": d["products"][sku].get("attributes", {}),
                "on_demand": [pd for term in d["terms"].get("OnDemand", {}).get(sku, {}).values()
                              for pd in term["priceDimensions"].values()],
            }
missing = [s for s in skus if s not in found]
(HERE / "price_list_extract.json").write_text(json.dumps({"skus": found, "not_found": missing}, indent=1))
print(f"{len(found)} skus extracted, {len(missing)} not found: {missing}")
