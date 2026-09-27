"""Executable warehouse job using synthetic data and no external services."""

import argparse
from decimal import Decimal
import json


def summarize_orders(orders):
    return {
        "order_count": len({order["order_id"] for order in orders}),
        "units_sold": sum(order["quantity"] for order in orders),
        "revenue_usd": str(sum(
            (Decimal(order["unit_price"]) * order["quantity"] for order in orders),
            Decimal("0.00"),
        )),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--environment", default="local")
    arguments = parser.parse_args()
    orders = [
        {"order_id": 1, "quantity": 2, "unit_price": "11.50"},
        {"order_id": 1, "quantity": 1, "unit_price": "5.00"},
        {"order_id": 2, "quantity": 1, "unit_price": "12.00"},
    ]
    print(json.dumps({"environment": arguments.environment, **summarize_orders(orders)}))


if __name__ == "__main__":
    main()