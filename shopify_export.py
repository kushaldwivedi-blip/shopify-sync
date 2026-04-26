import requests
import csv
import time
import os
from pymongo import MongoClient
from datetime import datetime, timezone

# ============================================================
#  CONFIG
# ============================================================
SHOP_NAME    = "kbwebm"
ACCESS_TOKEN = "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"
API_VERSION  = "2024-01"
MONGO_URI    = os.environ.get("MONGO_URI")
# ============================================================

BASE_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}"
HEADERS  = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

OUTPUT_FILE     = "shopify_orders.csv"
ORDERS_PER_PAGE = 250

CSV_FIELDS = [
    "order_id", "order_number", "created_at", "updated_at", "processed_at",
    "fulfillment_status", "financial_status", "order_status",
    "customer_id", "customer_email", "customer_first_name", "customer_last_name",
    "customer_phone", "customer_orders_count",
    "shipping_name", "shipping_address1", "shipping_address2",
    "shipping_city", "shipping_province", "shipping_zip", "shipping_country",
    "currency", "subtotal_price", "total_discounts", "total_tax",
    "total_shipping", "total_price", "payment_gateway", "refund_amount",
    "line_item_id", "line_item_product_id", "line_item_variant_id",
    "line_item_sku", "line_item_title", "line_item_variant_title",
    "line_item_quantity", "line_item_price", "line_item_total_discount",
    "line_item_vendor", "line_item_fulfillment_status",
]


# ── helpers ──────────────────────────────────────────────────

def parse_next_link(link_header: str):
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None


def calc_refund_total(refunds):
    total = 0.0
    for refund in refunds or []:
        for txn in refund.get("transactions", []):
            if txn.get("kind") in ("refund", "void"):
                total += float(txn.get("amount", 0))
    return round(total, 2)


# ── fetch ─────────────────────────────────────────────────────

def fetch_all_orders():
    orders = []
    url    = f"{BASE_URL}/orders.json"
    params = {
        "status": "any",
        "limit":  ORDERS_PER_PAGE,
        "fields": (
            "id,order_number,created_at,updated_at,processed_at,"
            "fulfillment_status,financial_status,cancel_reason,"
            "customer,shipping_address,email,"
            "currency,subtotal_price,total_discounts,total_tax,"
            "total_shipping_price_set,total_price,payment_gateway,"
            "refunds,line_items"
        ),
    }

    page = 1
    while url:
        print(f"  Fetching page {page}...", end=" ")
        resp = requests.get(url, headers=HEADERS, params=params)

        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 2))
            print(f"Rate limited — waiting {wait}s")
            time.sleep(wait)
            continue

        if resp.status_code == 401:
            print("\n❌ 401 Unauthorized — Access token galat hai.")
            raise SystemExit(1)

        resp.raise_for_status()
        batch = resp.json().get("orders", [])
        orders.extend(batch)
        print(f"{len(batch)} orders fetched (total: {len(orders)})")

        url    = parse_next_link(resp.headers.get("Link", ""))
        params = {}
        page  += 1
        time.sleep(0.5)

    return orders


# ── flatten ───────────────────────────────────────────────────

def flatten_order(order):
    rows     = []
    customer = order.get("customer") or {}
    shipping = order.get("shipping_address") or {}
    refunds  = order.get("refunds") or []

    shipping_amt = (
        order
        .get("total_shipping_price_set", {})
        .get("shop_money", {})
        .get("amount", "0")
    )

    base = {
        "order_id":           order.get("id"),
        "order_number":       order.get("order_number"),
        "created_at":         order.get("created_at"),
        "updated_at":         order.get("updated_at"),
        "processed_at":       order.get("processed_at"),
        "fulfillment_status": order.get("fulfillment_status") or "unfulfilled",
        "financial_status":   order.get("financial_status"),
        "order_status":       "cancelled" if order.get("cancel_reason") else "open",
        "customer_id":           customer.get("id"),
        "customer_email":        customer.get("email") or order.get("email"),
        "customer_first_name":   customer.get("first_name"),
        "customer_last_name":    customer.get("last_name"),
        "customer_phone":        customer.get("phone"),
        "customer_orders_count": customer.get("orders_count"),
        "shipping_name":     shipping.get("name"),
        "shipping_address1": shipping.get("address1"),
        "shipping_address2": shipping.get("address2"),
        "shipping_city":     shipping.get("city"),
        "shipping_province": shipping.get("province"),
        "shipping_zip":      shipping.get("zip"),
        "shipping_country":  shipping.get("country"),
        "currency":         order.get("currency"),
        "subtotal_price":   order.get("subtotal_price"),
        "total_discounts":  order.get("total_discounts"),
        "total_tax":        order.get("total_tax"),
        "total_shipping":   shipping_amt,
        "total_price":      order.get("total_price"),
        "payment_gateway":  order.get("payment_gateway"),
        "refund_amount":    calc_refund_total(refunds),
    }

    line_items = order.get("line_items") or []
    if not line_items:
        rows.append(base)
    else:
        for item in line_items:
            rows.append({
                **base,
                "line_item_id":                 item.get("id"),
                "line_item_product_id":         item.get("product_id"),
                "line_item_variant_id":         item.get("variant_id"),
                "line_item_sku":                item.get("sku"),
                "line_item_title":              item.get("title"),
                "line_item_variant_title":      item.get("variant_title"),
                "line_item_quantity":           item.get("quantity"),
                "line_item_price":              item.get("price"),
                "line_item_total_discount":     item.get("total_discount"),
                "line_item_vendor":             item.get("vendor"),
                "line_item_fulfillment_status": item.get("fulfillment_status") or "unfulfilled",
            })

    return rows


# ── export CSV ────────────────────────────────────────────────

def export_to_csv(orders):
    all_rows = []
    for order in orders:
        all_rows.extend(flatten_order(order))

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)

    return len(all_rows)


# ── save to MongoDB ───────────────────────────────────────────

def save_to_mongodb(orders):
    if not MONGO_URI:
        print("⚠️  MONGO_URI nahi mila — MongoDB skip kar raha hoon.")
        return

    client = MongoClient(MONGO_URI)
    db     = client["shopify"]
    col    = db["orders"]

    all_rows = []
    for order in orders:
        all_rows.extend(flatten_order(order))

    if all_rows:
        col.delete_many({})           # purana data delete karo
        col.insert_many(all_rows)     # naya data insert karo
        print(f"✅ MongoDB updated! {len(all_rows)} rows inserted.")
    else:
        print("⚠️  Koi data nahi mila insert karne ke liye.")

    # Last sync time update karo
    db["sync_log"].replace_one(
        {"_id": "last_sync"},
        {"_id": "last_sync", "synced_at": datetime.now(timezone.utc).isoformat()},
        upsert=True
    )

    client.close()


# ── main ──────────────────────────────────────────────────────

def main():
    print("=" * 50)
    print("  Shopify → CSV + MongoDB Export")
    print(f"  Store : {SHOP_NAME}.myshopify.com")
    print("=" * 50)

    print("\n📦 Fetching orders...")
    orders = fetch_all_orders()
    print(f"\n✅ Total orders fetched: {len(orders)}")

    print("\n💾 Writing CSV...")
    total_rows = export_to_csv(orders)
    print(f"✅ CSV ready! {total_rows} rows written.")

    print("\n🍃 Saving to MongoDB...")
    save_to_mongodb(orders)

    print("\n🎯 Done! PowerBI se MongoDB connect karo.")


if __name__ == "__main__":
    main()
