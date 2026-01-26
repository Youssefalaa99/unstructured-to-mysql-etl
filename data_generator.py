import json
import random
from faker import Faker
from datetime import datetime

fake = Faker()

def generate_transaction():
    # Pre-define a few products to simulate recurring product IDs
    products = [
        {"id": "PROD-001", "sku": "SKU-RED", "cat": "Apparel", "price": 19.99},
        {"id": "PROD-002", "sku": "SKU-BLU", "cat": "Apparel", "price": 24.50},
        {"id": "PROD-003", "sku": "SKU-GRN", "cat": "Home", "price": 89.00},
    ]

    # Pick 1-3 random products for this transaction
    selected_products = random.sample(products, random.randint(1, 3))
    
    line_items = []
    total_amt = 0
    
    for p in selected_products:
        qty = random.randint(1, 5)
        line_items.append({
            "product_id": p["id"],
            "sku": p["sku"],
            "category": p["cat"],
            "unit_price": p["price"],
            "quantity": qty
        })
        total_amt += (p["price"] * qty)

    # Build the nested structure
    transaction = {
        "transaction_id": f"TXN-{fake.uuid4()[:8].upper()}",
        "timestamp": fake.date_time_between(start_date='-30d', end_date='now').isoformat(),
        "store": {
            "store_id": random.choice(["STR-01", "STR-02"]),
            "location": fake.city(),
            "region": random.choice(["North", "South", "East", "West"])
        },
        "customer": {
            "customer_id": f"USR-{random.randint(100, 999)}",
            "personal_info": {
                "full_name": fake.name(),
                "email": fake.email()
            },
            "loyalty_member": random.choice([True, False])
        },
        "line_items": line_items,
        "payment": {
            "method": random.choice(["Credit Card", "PayPal", "Crypto"]),
            "currency": "USD",
            "total_amount": round(total_amt, 2),
            "tax": round(total_amt * 0.08, 2)
        }
    }
    return transaction

# Generate a list of 10 transactions
data_set = [generate_transaction() for _ in range(10)]

# Save to a file
with open('dummy_transactions.json', 'w') as f:
    json.dump(data_set, f, indent=4)