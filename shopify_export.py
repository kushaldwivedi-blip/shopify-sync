import requests
import csv
import time
import os
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone

# ============================================================
#  CONFIG
# ============================================================
SHOP_NAME    = "kbwebm"
ACCESS_TOKEN = "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"
API_VERSION  = "2024-01"
MONGO_URI    = "mongodb+srv://shopify_user:<db_password>@cluster0.kmgopfc.mongodb.net/?appName=Cluster0"
# ============================================================

BASE_URL = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}"
HEADERS  = {
    "X-Shopify-Access-Token": ACCESS_TOKEN,
    "Content-Type": "application/json",
}

ORDERS_PER_PAGE = 250

# ── Folder structure ─────────────────────────────────────────
FOLDERS = {
    "orders":       "orders",
    "products":     "products",
    "customers":    "customers",
    "draft_orders": "draft_orders",
}

def ensure_folders():
    for folder in FOLDERS.values():
        os.makedirs(folder, exist_ok=True)
    print("📁 Folders ready: orders/, products/, customers/, draft_orders/")


# ============================================================
#  CSV FIELDS
# ============================================================

ORDER_FIELDS = [
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

PRODUCT_FIELDS = [
    "product_id", "title", "vendor", "product_type", "status",
    "created_at", "updated_at", "published_at", "tags",
    "variant_id", "variant_title", "variant_sku", "variant_price",
    "variant_compare_at_price", "variant_inventory_quantity",
    "variant_inventory_policy", "variant_fulfillment_service",
    "variant_weight", "variant_weight_unit", "variant_requires_shipping",
    "variant_taxable", "variant_available",
]

CUSTOMER_FIELDS = [
    "customer_id", "email", "first_name", "last_name", "phone",
    "created_at", "updated_at", "orders_count", "total_spent",
    "verified_email", "tags", "currency", "accepts_marketing",
    "state", "note",
    "address_id", "address1", "address2", "city", "province",
    "zip", "country", "company", "is_default_address",
]

DRAFT_ORDER_FIELDS = [
    "draft_order_id", "order_number", "status", "created_at", "updated_at",
    "completed_at", "invoice_sent_at",
    "customer_id", "customer_email", "customer_first_name", "customer_last_name",
    "shipping_name", "shipping_address1", "shipping_address2",
    "shipping_city", "shipping_province", "shipping_zip", "shipping_country",
    "currency", "subtotal_price", "total_discounts", "total_tax",
    "total_shipping", "total_price",
    "line_item_id", "line_item_product_id", "line_item_variant_id",
    "line_item_sku", "line_item_title", "line_item_variant_title",
    "line_item_quantity", "line_item_price", "line_item_vendor",
]


# ============================================================
#  HELPERS
# ============================================================

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


def paginated_fetch(endpoint, root_key, params):
    """Generic paginated fetcher for any Shopify endpoint."""
    items = []
    url   = f"{BASE_URL}/{endpoint}.json"
    page  = 1

    while url:
        print(f"  Page {page}...", end=" ")
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
        batch = resp.json().get(root_key, [])
        items.extend(batch)
        print(f"{len(batch)} fetched (total: {len(items)})")

        url    = parse_next_link(resp.headers.get("Link", ""))
        params = {}          # pagination URL mein sab kuch already hota hai
        page  += 1
        time.sleep(0.5)

    return items


# ============================================================
#  FETCH FUNCTIONS
# ============================================================

def fetch_orders():
    return paginated_fetch(
        "orders", "orders",
        {
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
    )


def fetch_products():
    return paginated_fetch(
        "products", "products",
        {
            "limit":  ORDERS_PER_PAGE,
            "fields": (
                "id,title,vendor,product_type,status,"
                "created_at,updated_at,published_at,tags,variants"
            ),
        }
    )


def fetch_customers():
    return paginated_fetch(
        "customers", "customers",
        {
            "limit":  ORDERS_PER_PAGE,
            "fields": (
                "id,email,first_name,last_name,phone,"
                "created_at,updated_at,orders_count,total_spent,"
                "verified_email,tags,currency,accepts_marketing,"
                "state,note,addresses"
            ),
        }
    )


def fetch_draft_orders():
    return paginated_fetch(
        "draft_orders", "draft_orders",
        {
            "limit":  ORDERS_PER_PAGE,
            "fields": (
                "id,order_number,status,created_at,updated_at,"
                "completed_at,invoice_sent_at,"
                "customer,shipping_address,"
                "currency,subtotal_price,total_discounts,total_tax,"
                "total_shipping_price_set,total_price,line_items"
            ),
        }
    )


# ============================================================
#  FLATTEN FUNCTIONS
# ============================================================

def flatten_orders(orders):
    rows = []
    for order in orders:
        customer = order.get("customer") or {}
        shipping = order.get("shipping_address") or {}
        refunds  = order.get("refunds") or []
        shipping_amt = (
            order.get("total_shipping_price_set", {})
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


def flatten_products(products):
    rows = []
    for product in products:
        base = {
            "product_id":   product.get("id"),
            "title":        product.get("title"),
            "vendor":       product.get("vendor"),
            "product_type": product.get("product_type"),
            "status":       product.get("status"),
            "created_at":   product.get("created_at"),
            "updated_at":   product.get("updated_at"),
            "published_at": product.get("published_at"),
            "tags":         product.get("tags"),
        }
        variants = product.get("variants") or []
        if not variants:
            rows.append(base)
        else:
            for v in variants:
                rows.append({
                    **base,
                    "variant_id":                v.get("id"),
                    "variant_title":             v.get("title"),
                    "variant_sku":               v.get("sku"),
                    "variant_price":             v.get("price"),
                    "variant_compare_at_price":  v.get("compare_at_price"),
                    "variant_inventory_quantity":v.get("inventory_quantity"),
                    "variant_inventory_policy":  v.get("inventory_policy"),
                    "variant_fulfillment_service":v.get("fulfillment_service"),
                    "variant_weight":            v.get("weight"),
                    "variant_weight_unit":       v.get("weight_unit"),
                    "variant_requires_shipping": v.get("requires_shipping"),
                    "variant_taxable":           v.get("taxable"),
                    "variant_available":         v.get("inventory_quantity", 0) > 0,
                })
    return rows


def flatten_customers(customers):
    rows = []
    for customer in customers:
        base = {
            "customer_id":       customer.get("id"),
            "email":             customer.get("email"),
            "first_name":        customer.get("first_name"),
            "last_name":         customer.get("last_name"),
            "phone":             customer.get("phone"),
            "created_at":        customer.get("created_at"),
            "updated_at":        customer.get("updated_at"),
            "orders_count":      customer.get("orders_count"),
            "total_spent":       customer.get("total_spent"),
            "verified_email":    customer.get("verified_email"),
            "tags":              customer.get("tags"),
            "currency":          customer.get("currency"),
            "accepts_marketing": customer.get("accepts_marketing"),
            "state":             customer.get("state"),
            "note":              customer.get("note"),
        }
        addresses = customer.get("addresses") or []
        if not addresses:
            rows.append(base)
        else:
            for addr in addresses:
                rows.append({
                    **base,
                    "address_id":        addr.get("id"),
                    "address1":          addr.get("address1"),
                    "address2":          addr.get("address2"),
                    "city":              addr.get("city"),
                    "province":          addr.get("province"),
                    "zip":               addr.get("zip"),
                    "country":           addr.get("country"),
                    "company":           addr.get("company"),
                    "is_default_address":addr.get("default"),
                })
    return rows


def flatten_draft_orders(draft_orders):
    rows = []
    for order in draft_orders:
        customer = order.get("customer") or {}
        shipping = order.get("shipping_address") or {}
        shipping_amt = (
            order.get("total_shipping_price_set", {})
                 .get("shop_money", {})
                 .get("amount", "0")
        )
        base = {
            "draft_order_id":    order.get("id"),
            "order_number":      order.get("order_number"),
            "status":            order.get("status"),
            "created_at":        order.get("created_at"),
            "updated_at":        order.get("updated_at"),
            "completed_at":      order.get("completed_at"),
            "invoice_sent_at":   order.get("invoice_sent_at"),
            "customer_id":         customer.get("id"),
            "customer_email":      customer.get("email"),
            "customer_first_name": customer.get("first_name"),
            "customer_last_name":  customer.get("last_name"),
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
        }
        line_items = order.get("line_items") or []
        if not line_items:
            rows.append(base)
        else:
            for item in line_items:
                rows.append({
                    **base,
                    "line_item_id":           item.get("id"),
                    "line_item_product_id":   item.get("product_id"),
                    "line_item_variant_id":   item.get("variant_id"),
                    "line_item_sku":          item.get("sku"),
                    "line_item_title":        item.get("title"),
                    "line_item_variant_title":item.get("variant_title"),
                    "line_item_quantity":     item.get("quantity"),
                    "line_item_price":        item.get("price"),
                    "line_item_vendor":       item.get("vendor"),
                })
    return rows


# ============================================================
#  CSV EXPORT
# ============================================================

def get_timestamp():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

def export_csv(rows, folder, filename_prefix, fields):
    timestamp = get_timestamp()
    filepath  = os.path.join(folder, f"{filename_prefix}_{timestamp}.csv")
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✅ CSV saved: {filepath}  ({len(rows)} rows)")
    return filepath


# ============================================================
#  MONGODB UPSERT
# ============================================================

def upsert_to_mongodb(rows, collection_name, unique_key):
    """
    Upsert rows into MongoDB collection.
    unique_key: field name used to identify unique records (e.g. 'order_id')
    Purana data RAHEGA — sirf naya/updated records add/update honge.
    """
    if not MONGO_URI:
        print(f"  ⚠️  MONGO_URI nahi mila — {collection_name} MongoDB skip.")
        return

    client = MongoClient(MONGO_URI)
    db     = client["shopify"]
    col    = db[collection_name]

    if not rows:
        print(f"  ⚠️  {collection_name}: Koi data nahi.")
        client.close()
        return

    operations = [
        UpdateOne(
            {unique_key: row[unique_key]},   # filter — unique field se match karo
            {"$set": row},                    # update — ya insert karo
            upsert=True
        )
        for row in rows
        if row.get(unique_key) is not None    # unique key missing ho to skip
    ]

    result = col.bulk_write(operations, ordered=False)
    print(
        f"  ✅ MongoDB [{collection_name}]: "
        f"inserted={result.upserted_count}, "
        f"updated={result.modified_count}, "
        f"total processed={len(operations)}"
    )
    client.close()


def update_sync_log(synced_types):
    if not MONGO_URI:
        return
    client = MongoClient(MONGO_URI)
    db     = client["shopify"]
    db["sync_log"].replace_one(
        {"_id": "last_sync"},
        {
            "_id":        "last_sync",
            "synced_at":  datetime.now(timezone.utc).isoformat(),
            "synced_types": synced_types,
        },
        upsert=True
    )
    client.close()
    print("  ✅ Sync log updated.")


# ============================================================
#  MAIN
# ============================================================

SYNC_CONFIG = [
    {
        "name":        "Orders",
        "fetch_fn":    fetch_orders,
        "flatten_fn":  flatten_orders,
        "folder":      FOLDERS["orders"],
        "csv_prefix":  "orders",
        "fields":      ORDER_FIELDS,
        "mongo_col":   "orders",
        "unique_key":  "line_item_id",   # line_item_id unique hai per row
    },
    {
        "name":        "Products",
        "fetch_fn":    fetch_products,
        "flatten_fn":  flatten_products,
        "folder":      FOLDERS["products"],
        "csv_prefix":  "products",
        "fields":      PRODUCT_FIELDS,
        "mongo_col":   "products",
        "unique_key":  "variant_id",
    },
    {
        "name":        "Customers",
        "fetch_fn":    fetch_customers,
        "flatten_fn":  flatten_customers,
        "folder":      FOLDERS["customers"],
        "csv_prefix":  "customers",
        "fields":      CUSTOMER_FIELDS,
        "mongo_col":   "customers",
        "unique_key":  "customer_id",
    },
    {
        "name":        "Draft Orders",
        "fetch_fn":    fetch_draft_orders,
        "flatten_fn":  flatten_draft_orders,
        "folder":      FOLDERS["draft_orders"],
        "csv_prefix":  "draft_orders",
        "fields":      DRAFT_ORDER_FIELDS,
        "mongo_col":   "draft_orders",
        "unique_key":  "draft_order_id",
    },
]


def main():
    print("=" * 60)
    print("  Shopify → CSV + MongoDB Sync")
    print(f"  Store  : {SHOP_NAME}.myshopify.com")
    print(f"  Run at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    ensure_folders()
    print()

    synced_types = []

    for cfg in SYNC_CONFIG:
        name = cfg["name"]
        print(f"─── {name} ─────────────────────────────────")

        # 1. Fetch
        print(f"📦 Fetching {name}...")
        raw = cfg["fetch_fn"]()
        print(f"   Total fetched: {len(raw)}")

        # 2. Flatten
        rows = cfg["flatten_fn"](raw)
        print(f"   Total rows (after flatten): {len(rows)}")

        # 3. CSV
        print(f"💾 Writing CSV...")
        export_csv(rows, cfg["folder"], cfg["csv_prefix"], cfg["fields"])

        # 4. MongoDB Upsert
        print(f"🍃 Upserting to MongoDB [{cfg['mongo_col']}]...")
        upsert_to_mongodb(rows, cfg["mongo_col"], cfg["unique_key"])

        synced_types.append(name)
        print()

    # 5. Sync log
    print("📝 Updating sync log...")
    update_sync_log(synced_types)

    print()
    print("=" * 60)
    print("🎯 All done! PowerBI se MongoDB connect karo.")
    print("   Collections: orders | products | customers | draft_orders")
    print("=" * 60)


if __name__ == "__main__":
    main()
