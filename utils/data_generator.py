import json
import random
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_transaction():
    # Pre-define a few products to simulate recurring product IDs
    products = [
        {"code": "PROD-001", "sku": "SKU-RED", "cat": "Apparel", "price": 19.99},
        {"code": "PROD-002", "sku": "SKU-BLU", "cat": "Apparel", "price": 24.50},
        {"code": "PROD-003", "sku": "SKU-GRN", "cat": "Home", "price": 89.00},
    ]

    # Pick 1-3 random products for this transaction
    selected_products = random.sample(products, random.randint(1, 3))
    
    line_items = []
    total_amt = 0
    
    for p in selected_products:
        qty = random.randint(1, 5)
        line_items.append({
            "product_code": p["code"],
            "sku": p["sku"],
            "category": p["cat"],
            "unit_price": p["price"],
            "quantity": qty
        })
        total_amt += (p["price"] * qty)

    # Build the nested structure
    transaction = {
        "transaction_code": f"TXN-{fake.uuid4()[:8].upper()}",
        "timestamp": fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
        "store": {
            # "store_code": random.choice(["STR-01", "STR-02"]),
            "store_code": f"STR-{random.randint(10, 99)}",
            "location": fake.city(),
            "region": random.choice(["North", "South", "East", "West"])
        },
        "customer": {
            "customer_code": f"USR-{random.randint(100, 999)}",
            "personal_info": {
                "full_name": fake.name(),
                "email": fake.email()
            },
            "loyalty_member": random.choice([True, False])
        },
        "line_items": line_items,
        "payment": {
            "method": random.choice(["Credit Card", "PayPal", "Cash"]),
            "currency": "USD",
            "total_amount": round(total_amt, 2),
            "tax": round(total_amt * 0.08, 2)
        }
    }
    return transaction

# Generate a list of 10 transactions
data_set = [generate_transaction() for _ in range(10)]


# Save to a file
file_name = f"dummy_transactions_{datetime.now():%Y%m%d_%H%M%S}.json"
with open(file_name, 'w') as f:
    json.dump(data_set, f, indent=4)