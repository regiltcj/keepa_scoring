import sys
import logging
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery


from trend_scores.trend_scores import compute_trend_scores
from periodic_scores.periodic_scores import compute_periodic_scores
from snapshot_scores.snapshot_scores import compute_snapshot_scores


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
    # # Download query results.
    bi_brand_metrics_daily_qs = "SELECT * FROM " + schema + ".bi_brand_metrics_daily"
    bi_brand_metrics_snapshot_qs = "SELECT * FROM " + schema + ".bi_brand_metrics_snapshot"
    bi_brand_segmentation_qs = "SELECT * FROM " + schema + ".bi_brand_segmentation"
    bi_product_metrics_daily_qs = "SELECT * FROM " + schema + ".bi_product_metrics_daily"
    products_qs = "SELECT * FROM " + schema + ".products"
    product_offer_snapshots_qs = "SELECT * FROM " + schema + ".product_offer_snapshots"
    product_snapshots_qs = "SELECT * FROM " + schema + ".product_snapshots"

    df = bq_client.query(bi_brand_metrics_daily_qs).result().to_dataframe()
    df.to_csv("./datasets/bi_brand_metrics_daily.csv")
    print("Wrote bi_brand_metrics_daily_qs")
    df = bq_client.query(bi_brand_metrics_snapshot_qs).result().to_dataframe()
    df.to_csv("./datasets/bi_brand_metrics_snapshot.csv")
    print("Wrote bi_brand_metrics_snapshot_qs")
    df = bq_client.query(bi_brand_segmentation_qs).result().to_dataframe()
    df.to_csv("./datasets/bi_brand_segmentation.csv")
    print("Wrote bi_brand_segmentation_qs")
    df = bq_client.query(bi_product_metrics_daily_qs).result().to_dataframe()
    df.to_csv("./datasets/bi_product_metrics_daily.csv")
    print("Wrote bi_product_metrics_daily_qs")
    df = bq_client.query(products_qs).result().to_dataframe()
    df.to_csv("./datasets/products.csv")
    print("Wrote products_qs")
    df = bq_client.query(product_offer_snapshots_qs).result().to_dataframe()
    df.to_csv("./datasets/product_offer_snapshots.csv")
    print("Wrote product_offer_snapshots_qs")
    df = bq_client.query(product_snapshots_qs).result().to_dataframe()
    df.to_csv("./datasets/product_snapshots.csv")
    print("Wrote product_snapshots_qs")

    # Creating a dataframe of all the unique brands for which we have the sales data for
    products = pd.read_csv("./datasets/products.csv")
    if brand == "":
        brands = pd.DataFrame(products.loc[~products["brand"].isna(), "brand"])
        brands.drop_duplicates(ignore_index=True, inplace=True)
    else:
        brands = pd.DataFrame(products.loc[products["brand"].eq(brand), "brand"])
        if brands.shape[0] == 0:
            logger.error(f"Given brand name ({brand}) does not exists")
            exit()
        brands.drop_duplicates(ignore_index=True, inplace=True)

    logger.info("Computing trend metrics")
    trend_scores = compute_trend_scores(brands)
    logger.info("Computing periodic metrics")
    periodic_scores = compute_periodic_scores(brands)
    logger.info("Computing snapshot metrics")
    snapshot_scores = compute_snapshot_scores(brands)

    final_scores = pd.merge(trend_scores, periodic_scores, on="brand", how="inner", validate="one_to_one")
    final_scores = pd.merge(final_scores, snapshot_scores, on="brand", how="inner", validate="one_to_one")
    logger.info("Writing final score to big query tables")
    if brand != "":
        #delete_brand_qs = "DELETE FROM " + schema + ".bi_brand_scores WHERE brand = '" + brand + "'"
        #bq_client = bigquery.Client.from_service_account_json(credential_json)
        #query_job = bq_client.query(delete_brand_qs)
        #query_job.result()
        final_scores.to_gbq(schema + ".bi_brand_scores", project_id=project_id, if_exists="append", credentials=credentials)
    else:
        final_scores.to_gbq(schema + ".bi_brands_scores", project_id=project_id, if_exists="replace", credentials=credentials)
    logger.info("Execution complete")
