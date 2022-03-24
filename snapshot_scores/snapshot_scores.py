import logging
import pandas as pd
from trend_scores.utils import scale_data
from sklearn.preprocessing import MinMaxScaler
from snapshot_scores.utils import get_weighted_value

logger = logging.getLogger()


def compute_snapshot_scores(brands):
    scaler = MinMaxScaler()
    bi_brand_metrics_snapshot = pd.read_csv("./datasets/bi_brand_metrics_snapshot.csv")
    logger.info("Evaluating seller_reviews_count score")
    bi_brand_metrics_snapshot["snapshot_seller_reviews_count"] = scale_data(
        bi_brand_metrics_snapshot[["avg_seller_reviews_count"]],
        scaler)
    brands = pd.merge(brands, bi_brand_metrics_snapshot.loc[:, ["brand", "snapshot_seller_reviews_count"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed seller_reviews_count score")
    # ------------------------------------------------------------------------------------------- #
    logger.info("Evaluating seller_reviews_score score")
    bi_brand_metrics_snapshot["snapshot_seller_reviews_score"] = scale_data(
        bi_brand_metrics_snapshot[["avg_seller_reviews_score"]],
        scaler)
    brands = pd.merge(brands, bi_brand_metrics_snapshot.loc[:, ["brand", "snapshot_seller_reviews_score"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed seller_reviews_score score")

    del bi_brand_metrics_snapshot

    ###############################################################################################

    logger.info("Evaluating product_reviews_count score")
    bi_brand_segmentation = pd.read_csv("./datasets/bi_brand_segmentation.csv")
    bi_brand_segmentation["snapshot_product_reviews_count"] = scale_data(bi_brand_segmentation[
                                                                             ["avg_product_reviews_count"]], scaler)
    brands = pd.merge(brands, bi_brand_segmentation.loc[:, ["brand", "snapshot_product_reviews_count"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed product_reviews_count score")
    # ------------------------------------------------------------------------------------------- #
    logger.info("Evaluating product_reviews_score score")
    bi_brand_segmentation["snapshot_product_reviews_score"] = scale_data(bi_brand_segmentation[
                                                                             ["avg_product_reviews_score"]], scaler)
    brands = pd.merge(brands, bi_brand_segmentation.loc[:, ["brand", "snapshot_product_reviews_score"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed product_reviews_score score")

    del bi_brand_segmentation

    ###############################################################################################

    product_sellers = pd.read_csv("./datasets/product_sellers.csv")
    product_avg_items = pd.read_csv("./datasets/product_avg_items.csv")
    logger.info("Evaluating number_of_sellers score")

    number_of_sellers = get_weighted_value(product_sellers, product_avg_items, "seller_count", "daily_items")
    number_of_sellers.rename(columns={0: "sellers"}, inplace=True)
    del product_sellers
    del product_avg_items

    mean = number_of_sellers["sellers"].mean()
    std = number_of_sellers["sellers"].std()
    # Finding the z-score - the distance of the value from the mean value as a measure of standard deviation
    number_of_sellers["snapshot_number_of_sellers"] = ((number_of_sellers["sellers"] - mean) / std).abs()

    # Rescaling so that farther the distance, lower the score
    max_number_of_sellers = number_of_sellers["snapshot_number_of_sellers"].max()
    number_of_sellers["snapshot_number_of_sellers"] = max_number_of_sellers - number_of_sellers[
        "snapshot_number_of_sellers"]
    brands = pd.merge(brands, number_of_sellers.loc[:, ["brand", "snapshot_number_of_sellers"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed number_of_sellers score")

    ###############################################################################################

    logger.info("Evaluating price_ratio score")
    product_sellers_price_ratio = pd.read_csv("./datasets/product_sellers_price_ratio.csv")
    product_avg_sales = pd.read_csv("./datasets/product_avg_sales.csv")

    pricing = get_weighted_value(product_sellers_price_ratio, product_avg_sales, "price_ratio", "daily_sales")
    pricing.rename(columns={0: "snapshot_price_ratio"}, inplace=True)
    brands = pd.merge(brands, pricing, on="brand", how="left", validate="one_to_one")
    logger.info("Computed price_ratio score")

    del product_sellers_price_ratio
    del product_avg_sales

    logger.info("Completed evaluation of snapshot metrics")

    return brands
