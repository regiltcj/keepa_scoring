import logging
import pandas as pd
from google.oauth2 import service_account

from trend_scores.trend_scores import compute_trend_scores
from periodic_scores.periodic_scores import compute_periodic_scores
from snapshot_scores.snapshot_scores import compute_snapshot_scores

credentials = service_account.Credentials.from_service_account_file("keepa-etl-1-eb38919495bf.json",)
project_id = credentials.project_id
schema = "CS_1_1069664_151121"
logging.basicConfig(filename="log.log", filemode="w", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.info("Execution starts")
logger.info("Reading data from big query tables")

# Download query results.
bi_brand_metrics_daily_qs = "SELECT * FROM " + schema + ".bi_brand_metrics_daily"
bi_brand_metrics_snapshot_qs = "SELECT * FROM " + schema + ".bi_brand_metrics_snapshot"
bi_brand_segmentation_qs = "SELECT * FROM " + schema + ".bi_brand_segmentation"
bi_product_metrics_daily_qs = "SELECT * FROM " + schema + ".bi_product_metrics_daily"
products_qs = "SELECT * FROM " + schema + ".products"
product_offer_snapshots_qs = "SELECT * FROM " + schema + ".product_offer_snapshots"
product_snapshots_qs = "SELECT * FROM " + schema + ".product_snapshots"

bi_brand_metrics_daily = pd.read_gbq(bi_brand_metrics_daily_qs, project_id=project_id, credentials=credentials)
bi_brand_metrics_snapshot = pd.read_gbq(bi_brand_metrics_snapshot_qs, project_id=project_id, credentials=credentials)
bi_brand_segmentation = pd.read_gbq(bi_brand_segmentation_qs, project_id=project_id, credentials=credentials)
bi_product_metrics_daily = pd.read_gbq(bi_product_metrics_daily_qs, project_id=project_id, credentials=credentials)
products = pd.read_gbq(products_qs, project_id=project_id, credentials=credentials)
product_offer_snapshots = pd.read_gbq(product_offer_snapshots_qs, project_id=project_id, credentials=credentials)
product_snapshots = pd.read_gbq(product_snapshots_qs, project_id=project_id, credentials=credentials)

# Converting numeric datatype to float
bi_brand_metrics_daily["daily_items"] = bi_brand_metrics_daily["daily_items"].astype("float")
bi_brand_metrics_daily["daily_items_share"] = bi_brand_metrics_daily["daily_items_share"].astype("float")

# Creating a dataframe of all the unique brands for which we have the sales data for
brands = pd.DataFrame(products.loc[~products["brand"].isna(), "brand"])
brands.drop_duplicates(ignore_index=True, inplace=True)

logger.info("Computing trend metrics")
trend_scores = compute_trend_scores(bi_brand_metrics_daily, bi_product_metrics_daily, products, brands)
logger.info("Computing periodic metrics")
periodic_scores = compute_periodic_scores(bi_brand_metrics_daily, bi_product_metrics_daily, product_snapshots, products, brands)
logger.info("Computing snapshot metrics")
snapshot_scores = compute_snapshot_scores(bi_product_metrics_daily, bi_brand_metrics_snapshot,
                                          bi_brand_segmentation, products, product_offer_snapshots, brands)

final_scores = pd.merge(trend_scores, periodic_scores, on="brand", how="inner", validate="one_to_one")
final_scores = pd.merge(final_scores, snapshot_scores, on="brand", how="inner", validate="one_to_one")
final_scores.to_csv("./final_scores.csv")
logger.info("Writing final score to big query tables")
final_scores.to_gbq(schema + ".bi_brand_scores", project_id=project_id, if_exists="replace")
logger.info("Execution complete")
