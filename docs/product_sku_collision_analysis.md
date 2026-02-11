# Product SKU Duplication Analysis

## Executive Summary
An analysis of `data/products.csv` was conducted to validate the handling of duplicate SKUs. The findings confirm that duplicate SKUs fall into three categories: **Duplicate Records** (same Product ID) and **SKU Collisions** (different Product IDs sharing a SKU). 

Contrary to the hypothesis of "versioning" (where one SKU has multiple rows representing updates), the data shows that SKU duplicates largely represent unrelated products (e.g., a Tablet and a Lipstick) sharing the same SKU.

## Key Findings

### 1. Duplicate Records (Will be fixed by Pipeline)
*   **Pattern:** Same `sku`, Same `product_id`.
*   **Handling:** The current `SilverProcessor` logic correctly deduplicates these based on `product_id`.
*   **Examples:** `SKU-74722` (Advanced Workout Leggings), `SKU-66060`.

### 2. SKU Collisions (Persist in Pipeline)
*   **Pattern:** Same `sku`, **Different** `product_id`.
*   **Status:** Both records can be `is_active=True`.
*   **Impact:** 9 identified SKUs have multiple **active** products associated with them.
*   **Observation:** These are often unrelated products, suggesting an upstream data quality issue (random assignment or recycling) rather than versioning.

#### Critical Examples (Multiple Active)
| SKU | Product A (Active) | Product B (Active) |
| :--- | :--- | :--- |
| **SKU-86765** | Smart Smart Watch (PRD-000239) | Compact Fitness Tracker (PRD-000482) |
| **SKU-43175** | Premium Food Processor (PRD-000178) | Essential Casual Sneakers (PRD-000251) |
| **SKU-82085** | Smart Science Kit (PRD-000165) | Professional Knife Set (PRD-000196) |

### 3. "Valid" Versioning (One Active, One Inactive)
*   **Pattern:** Same `sku`, Different `product_id`, mixed `is_active` status.
*   **Observation:** Even here, the products are often unrelated.
*   **Example:** `SKU-30204` links `Essential Tablet Pro` (Active) and `Classic Lipstick Collection` (Inactive).
*   **Conclusion:** This is likely **not** valid versioning, but coincidentally mixed status SKU collisions.

## Recommendations
1.  **Pipeline Logic:** Maintain current behavior (deduplicate on `product_id` only). Attempting to deduplicate on SKU would result in data loss of valid active products (e.g. deleting the Sneakers because the Food Processor exists).
2.  **Data Quality Tagging:** In the future, consider adding a `dq_collision_warning` flag to the Silver/Gold layer to indicate SKUs that are not unique.
3.  **Graph Modeling:** In SurrealDB, ensure queries looking up by `sku` are aware they may return multiple distinct products.

## Statistics
*   Total Records: 505
*   Unique SKUs: 483
*   SKUs with Duplicates: 21
*   SKUs with Multiple Active Products (Collisions): ~9 (excluding pure duplicates)
