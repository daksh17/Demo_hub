/**
 * Demo indexes for MongoDB (mongosh). ESR hint: Equality fields first, then Sort, then Range
 * in compound indexes. Run via prepare-demo-collections.sh after collections exist.
 *
 * Collections:
 * - demo.demo_items / demo_items_from_kafka — Debezium hub (sharded on {_id: "hashed"})
 * - demo.scenario_products — hub scenario catalog (not sharded; unique sku safe)
 */
const d = db.getSiblingDB("demo");

function ix(coll, keys, opts = {}) {
  const c = d.getCollection(coll);
  const name = opts.name || Object.entries(keys).map(([k, v]) => `${k}_${v}`).join("_");
  const res = c.createIndex(keys, { ...opts, name });
  print(`createIndex ${coll}: ${name} -> ${JSON.stringify(res)}`);
}

// --- scenario_products (hub Multi-DB scenario) — not sharded ---
["scenario_products"].forEach((coll) => {
  if (!d.getCollectionNames().includes(coll)) {
    d.createCollection(coll);
    print("created empty collection: " + coll);
  }
});

// E: sku equality / lookup; unique
ix("scenario_products", { sku: 1 }, { name: "esr_sku_unique", unique: true });
// Sort feed: op_pipeline uses .sort({ updated_at: -1 })
ix("scenario_products", { updated_at: -1, sku: 1 }, { name: "esr_updated_sku" });
// ESR-style compound: equality category, range on price, sort tie-breaker sku
ix(
  "scenario_products",
  { category: 1, unit_price_cents: 1, sku: 1 },
  { name: "esr_category_price_sku" }
);
// Partial: in-stock catalog slices (Equality stock filter + sort)
ix(
  "scenario_products",
  { category: 1, sku: 1 },
  { name: "partial_in_stock", partialFilterExpression: { stock_units: { $gt: 0 } } }
);
// Warehouse + sku for operational filters
ix("scenario_products", { warehouse: 1, sku: 1 }, { name: "esr_warehouse_sku" });
// Text search on title (Atlas-free text index; good for demo queries)
try {
  d.scenario_products.createIndex({ title: "text", description: "text" }, { name: "text_title_desc", weights: { title: 10, description: 1 } });
  print("createIndex scenario_products: text_title_desc -> ok");
} catch (e) {
  print("text index skip: " + e);
}

// --- demo_items (Debezium source; hashed shard key _id) ---
if (d.getCollectionNames().includes("demo_items")) {
  // Name is common in filters / hub workload; compound with _id helps covered plans on sharded col
  ix("demo_items", { name: 1, _id: 1 }, { name: "esr_name_id" });
  ix("demo_items", { name: 1, qty: 1 }, { name: "esr_name_qty" });
}

// --- demo_items_from_kafka (sink collection) ---
if (d.getCollectionNames().includes("demo_items_from_kafka")) {
  ix("demo_items_from_kafka", { name: 1, _id: 1 }, { name: "sink_name_id" });
}

print("demo Mongo indexes applied.");
