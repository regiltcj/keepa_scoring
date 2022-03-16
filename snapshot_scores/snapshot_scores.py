import logging
import pandas as pd
from trend_scores.utils import scale_data
from sklearn.preprocessing import MinMaxScaler
from snapshot_scores.utils import get_number_of_sellers, get_snapshot_pricing

logger = logging.getLogger()


def compute_snapshot_scores(bi_product_metrics_daily, bi_brand_metrics_snapshot, bi_brand_segmentation, products,
                            product_offer_snapshots, brands):

    scaler = MinMaxScaler()
    bi_brand_metrics_snapshot = pd.read_csv("./datasets/bi_brand_metrics_snapshot.csv")
    logger.info("Evaluating seller_reviews_count score")
    seller_reviews_count = bi_brand_metrics_snapshot.loc[:, ["brand", "avg_seller_reviews_count"]]
    seller_reviews_count["snapshot_seller_reviews_count"] = scale_data(
        seller_reviews_count[["avg_seller_reviews_count"]],
        scaler)
    brands = pd.merge(brands, seller_reviews_count.loc[:, ["brand", "snapshot_seller_reviews_count"]], on="brand",
                      how="left", validate="one_to_one")
    del seller_reviews_count
    logger.info("Computed seller_reviews_count score")

    logger.info("Evaluating seller_reviews_score score")
    seller_reviews_score = bi_brand_metrics_snapshot.loc[:, ["brand", "avg_seller_reviews_score"]]
    seller_reviews_score["snapshot_seller_reviews_score"] = scale_data(
        seller_reviews_score[["avg_seller_reviews_score"]],
        scaler)
    brands = pd.merge(brands, seller_reviews_score.loc[:, ["brand", "snapshot_seller_reviews_score"]], on="brand",
                      how="left", validate="one_to_one")
    del seller_reviews_score
    logger.info("Computed seller_reviews_score score")
    
    del bi_brand_metrics_snapshot
    
    logger.info("Evaluating product_reviews_count score")
    bi_brand_segmentation = pd.read_csv("./datasets/bi_brand_segmentation.csv")
    product_reviews_count = bi_brand_segmentation.loc[:, ["brand", "avg_product_reviews_count"]]
    product_reviews_count["snapshot_product_reviews_count"] = scale_data(product_reviews_count[
                                                                             ["avg_product_reviews_count"]], scaler)
    brands = pd.merge(brands, product_reviews_count.loc[:, ["brand", "snapshot_product_reviews_count"]], on="brand",
                      how="left", validate="one_to_one")
    logger.info("Computed product_reviews_count score")

    logger.info("Evaluating product_reviews_score score")
    product_reviews_score = bi_brand_segmentation.loc[:, ["brand", "avg_product_reviews_score"]]
    product_reviews_score["snapshot_product_reviews_score"] = scale_data(product_reviews_score[
                                                                             ["avg_product_reviews_score"]], scaler)
    brands = pd.merge(brands, product_reviews_score.loc[:, ["brand", "snapshot_product_reviews_score"]], on="brand",
                      how="left", validate="one_to_one")
    del bi_brand_segmentation
    logger.info("Computed product_reviews_score score")
    
    
    products = pd.read_csv("./datasets/products.csv")
    product_offer_snapshots = pd.read_csv("./datasets/product_offer_snapshots.csv")
    bi_product_metrics_daily = pd.read_csv("./datasets/bi_product_metrics_daily.csv")
    logger.info("Evaluating number_of_sellers score")
    number_of_sellers = get_number_of_sellers(products, product_offer_snapshots, bi_product_metrics_daily)
    del products
    del product_offer_snapshots
    del bi_product_metrics_daily
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

    logger.info("Evaluating price_ratio score")
    products = pd.read_csv("./datasets/products.csv")
    product_offer_snapshots = pd.read_csv("./datasets/product_offer_snapshots.csv")
    bi_product_metrics_daily = pd.read_csv("./datasets/bi_product_metrics_daily.csv")
    
    pricing = get_snapshot_pricing(products, bi_product_metrics_daily, product_offer_snapshots)
    brands = pd.merge(brands, pricing, on="brand", how="left", validate="one_to_one")
    logger.info("Computed price_ratio score")
    logger.info("Completed evaluation of snapshot metrics")
    
    del products
    del product_offer_snapshots
    del bi_product_metrics_daily
    return brands
