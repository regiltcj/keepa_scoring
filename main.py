import sys
import logging
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from trend_scores.trend_scores import compute_trend_scores
from periodic_scores.periodic_scores import compute_periodic_scores
from snapshot_scores.snapshot_scores import compute_snapshot_scores

# CS_1_1069664_151121
def print_incorrect_usage_message(message):
    logger.error(message)
    print(message)
    print("Usage: ")


logging.basicConfig(filename="log.log", filemode="w", format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.info("Execution starts")

if len(sys.argv) < 4:
    print_incorrect_usage_message("Incorrect number of arguments")
    exit()
else:
    brand = ""
    credential_json = sys.argv[1].lower()
    if credential_json[-5:] != ".json" and not (credential_json in ["d", "default"]):
        print_incorrect_usage_message("Not a valid file name for credentials")
        exit()
    schema = sys.argv[2]
    brand_analysis = sys.argv[3].lower()
    if not (brand_analysis in ["y", "n", "yes", "no"]):
        print_incorrect_usage_message("Not a valid value for individual brand analysis parameter")
        exit()
    if brand_analysis in ["y", "yes"]:
        if len(sys.argv) < 5:
            print_incorrect_usage_message("Brand name not provided")
            exit()
        brand = sys.argv[4]
    elif brand_analysis in ["n", "no"]:
        if len(sys.argv) > 4:
            print_incorrect_usage_message("Incorrect number of arguments. Brand name is provided though not necessary")
            exit()

    credentials = service_account.Credentials.from_service_account_file(credential_json)
    project_id = credentials.project_id
    bq_client = bigquery.Client.from_service_account_json(credential_json)

    if brand != " ":
        logger.info(f"Performing analysis for the '{brand}' brand '{schema}' of '{project_id}' project")
    else:
        logger.info(f"Performing analysis for the {schema} in {project_id} project")

    logger.info("Reading data from big query tables")
    # Download query results.
    source_tables = ["bi_brand_metrics_daily", "products", "bi_product_metrics_daily", "product_snapshots",
                     "bi_brand_metrics_snapshot", "bi_brand_segmentation", "product_offer_snapshots"]

    bi_brand_metrics_daily_qs = "SELECT * FROM " + schema + ".bi_brand_metrics_daily"
    brands_qs = "SELECT DISTINCT brand from " + schema + ".products"
    product_brands_daily_qs = "SELECT bp.product_id, p.brand, bp.sample_date, bp.new_price, bp.daily_sales FROM " + \
                              schema + ".bi_product_metrics_daily bp INNER JOIN " + \
                              schema + ".products p ON bp.product_id = p.product_id"
    out_of_stock_qs = "SELECT p.product_id, p.brand, ps.out_of_stock_pct FROM " + schema + \
                      ".product_snapshots ps INNER JOIN " + schema + ".products p ON ps.product_id = p.product_id"
    bi_brand_metrics_snapshot_qs = "SELECT brand, avg_seller_reviews_count, avg_seller_reviews_score FROM " + schema + \
                                   ".bi_brand_metrics_snapshot"
    bi_brand_segmentation_qs = "SELECT brand, avg_product_reviews_count, avg_product_reviews_score FROM " + schema + \
                               ".bi_brand_segmentation"
    product_sellers_price_ratio_qs = "SELECT ps.product_id, p.brand, " \
                                     "CASE WHEN MAX(ps.recent_price) != 0 THEN " \
                                     "MIN(ps.recent_price)/MAX(ps.recent_price) ELSE 0 END AS price_ratio FROM " + \
                                     schema + ".product_offer_snapshots ps INNER JOIN " + schema + \
                                     ".products p ON ps.product_id = p.product_id WHERE ps.recent_price != -1 " \
                                     "GROUP BY ps.product_id, p.brand"
    product_avg_sales_qs = "SELECT product_id, AVG(daily_sales) AS daily_sales FROM " + schema + \
                           ".bi_product_metrics_daily GROUP BY product_id"
    product_sellers = "SELECT ps.product_id, p.brand, COUNT(ps.seller_id) AS seller_count FROM " + schema + \
                      ".product_offer_snapshots ps INNER JOIN " + schema + \
                      ".products p ON ps.product_id = p.product_id GROUP BY ps.product_id, p.brand"
    product_avg_items = "SELECT product_id, AVG(daily_items) AS daily_items FROM " + schema + \
                        ".bi_product_metrics_daily GROUP BY product_id"

    try:
        df = bq_client.query(bi_brand_metrics_daily_qs).result().to_dataframe()
        df.to_csv("./datasets/bi_brand_metrics_daily.csv")
        df = bq_client.query(brands_qs).result().to_dataframe()
        df.to_csv("./datasets/brands.csv")
        df = bq_client.query(product_brands_daily_qs).result().to_dataframe()
        df.to_csv("./datasets/product_brands_daily.csv")
        df = bq_client.query(out_of_stock_qs).result().to_dataframe()
        df.to_csv("./datasets/out_of_stock.csv")
        df = bq_client.query(bi_brand_metrics_snapshot_qs).result().to_dataframe()
        df.to_csv("./datasets/bi_brand_metrics_snapshot.csv")
        df = bq_client.query(bi_brand_segmentation_qs).result().to_dataframe()
        df.to_csv("./datasets/bi_brand_segmentation.csv")
        df = bq_client.query(product_sellers_price_ratio_qs).result().to_dataframe()
        df.to_csv("./datasets/product_sellers_price_ratio.csv")
        df = bq_client.query(product_avg_sales_qs).result().to_dataframe()
        df.to_csv("./datasets/product_avg_sales.csv")
        df = bq_client.query(product_sellers).result().to_dataframe()
        df.to_csv("./datasets/product_sellers.csv")
        df = bq_client.query(product_avg_items).result().to_dataframe()
        df.to_csv("./datasets/product_avg_items.csv")
    except NotFound as e:
        logger.error("Table does not exist in the schema " + schema)
        logger.error(e)
        exit()

    # Creating a dataframe of all the unique brands for which we have the sales data for
    brands = pd.read_csv("./datasets/brands.csv")
    if brand == "":
        brands = brands.loc[~brands["brand"].isna(), :]
    else:
        brands = brands.loc[brands["brand"].eq(brand), :]
        if brands.shape[0] == 0:
            logger.error(f"Given brand name ({brand}) does not exists")
            exit()
    brands.drop_duplicates(ignore_index=True, inplace=True)
    brands.drop(columns="Unnamed: 0", inplace=True)

    logger.info("Computing trend metrics")
    trend_scores = compute_trend_scores(brands)
    trend_scores.to_csv("./scores/trend_scores.csv")
    logger.info("Computing periodic metrics")
    periodic_scores = compute_periodic_scores(brands)
    periodic_scores.to_csv("./scores/periodic_scores.csv")
    logger.info("Computing snapshot metrics")
    snapshot_scores = compute_snapshot_scores(brands)
    snapshot_scores.to_csv("./scores/snapshot_scores.csv")

    final_scores = pd.merge(trend_scores, periodic_scores, on="brand", how="inner", validate="one_to_one")
    final_scores = pd.merge(final_scores, snapshot_scores, on="brand", how="inner", validate="one_to_one")
    logger.info("Writing final score to big query tables")
    final_scores.to_csv("./scores/scores_tbd.csv")
    if brand != "":
        try:
            delete_brand_qs = "DELETE FROM " + schema + ".bi_brand_scores WHERE brand = '" + brand + "'"
            query_job = bq_client.query(delete_brand_qs)
            query_job.result()
            # Check the count of query job and change the log accordingly
            logger.info("Updating the scores for the brand " + brand + " in bi_brand_scores table")
        except NotFound as e:
            logger.info("Target table bi_brand_scores does not exist and hence creating the table")

        final_scores.to_gbq(schema + ".bi_brand_scores", project_id=project_id, if_exists="append",
                            credentials=credentials)
    else:
        final_scores.to_gbq(schema + ".bi_brands_scores", project_id=project_id, if_exists="replace",
                            credentials=credentials)
    logger.info("Execution complete")
