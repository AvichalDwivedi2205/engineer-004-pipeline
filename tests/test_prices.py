"""Every AWS list price used by tools/cost_sheet.py must equal the Price List API extract."""
import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("cost_sheet", ROOT / "tools" / "cost_sheet.py")
cost_sheet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cost_sheet)
EXTRACT = json.loads((ROOT / "evidence" / "price_list_extract.json").read_text())["skus"]

# cost_sheet key -> (sku, multiplier from the sku's unit to the cost_sheet unit)
SKU_MAP = {
    "fargate_arm_vcpu_h": ("XSZATS4VYMDC9CYN", 1), "fargate_arm_gb_h": ("UNH9KPQP7W7C66C9", 1),
    "alb_h": ("37CUWUT8GSNQEPUV", 1), "alb_lcu_h": ("P2XGEJ8N3KU52WA8", 1),
    "kds_shard_h": ("7RXVY4V5SG8NQC38", 1), "kds_ext_retention_shard_h": ("9DT967X6YQX3AYMB", 1),
    "kds_put_unit_per_m": ("MAFXKQJ2YAQM6UDY", None),
    "kds_od_stream_h": ("S69RFKAH58MYJ6TF", 1), "kds_od_in_gb": ("826E7FJQWVKN3CBH", 1),
    "kds_od_out_gb": ("PNHYMNK9TM59MTU8", 1), "kds_od_ext_retention_gb_month": ("59DG9NNVUBGN3C5M", 1),
    "lambda_arm_gb_s": ("72SBSFWPMDTH8S3J", 1), "lambda_req_per_m": ("K7BX6567RJ67A2KE", 1e6),
    "valkey_r7g_xl_h": ("PYHN6K7E6KP5TYPE", 1),
    "privatelink_endpoint_az_h": ("EN2N5TATXE673A3B", 1), "privatelink_gb": ("DQYYEZBCWSW5XS2R", 1),
    "firehose_s3_batch_gb": ("B674W3KVY282C9AV", 1), "firehose_iceberg_gb": ("4B6YDGGHJ68E9SNF", 1),
    "s3_gb_month": ("WP9ANXZGBYYSGJEA", 1), "rds_gp3_maz_gb_month": ("J7S7KD4WFDNQWKNX", 1),
    "egress_internet_gb": ("HQEH3ZWJVT46JHRG", 1),
    "athena_tb": ("B6WHE3FUNCDQVVX4", 1), "rds_pg_r7g_large_maz_h": ("QPKXCKEKNV5DW3QA", 1),
    "cw_logs_gb": ("96K55R2PV3ZZZ3AM", 1), "cw_metric_month": ("KG586CTNGQ4VRZKZ", 1),
    "ec2_r7g_2xl_h": ("4W55HJV9VC4VWG8P", 1), "ec2_m7g_xl_h": ("Y7X6HJY9G859NU23", 1),
    "gp3_gb_month": ("JG3KUJMBRGHV3N8G", 1),
}


def first_tier_usd(sku):
    dims = EXTRACT[sku]["on_demand"]
    dim = next((d for d in dims if d.get("beginRange") in ("0", None)), dims[0])
    return float(dim["pricePerUnit"]["USD"]), dim["unit"], dim["description"]


class PriceTests(unittest.TestCase):
    def test_cost_sheet_prices_match_price_list(self):
        for key, (sku, mult) in SKU_MAP.items():
            usd, unit, desc = first_tier_usd(sku)
            if mult is None:  # PUT payload units: the sku may be priced per unit or per million
                mult = 1e6 if usd < 1e-3 else 1
            with self.subTest(key=key, sku=sku, desc=desc):
                self.assertAlmostEqual(cost_sheet.P[key], usd * mult, places=9)

    def test_every_aws_price_in_the_sheet_is_mapped(self):
        non_aws = {"ch_scale_unit_h", "ch_storage_tb_month"}
        self.assertEqual(set(cost_sheet.P) - non_aws, set(SKU_MAP))


if __name__ == "__main__":
    unittest.main()
