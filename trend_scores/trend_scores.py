import logging
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from trend_scores.utils import scale_data, compute_trend_score

logger = logging.getLogger()


def compute_trend_scores(brands):
    brand_list = list(brands["brand"])

    bi_brand_metrics_daily = pd.read_csv("./datasets/bi_brand_metrics_daily.csv")

    # Converting the sample_date to datetime
    bi_brand_metrics_daily.loc[:, "sample_date"] = pd.to_datetime(bi_brand_metrics_daily["sample_date"])
    bi_brand_metrics_daily.loc[:, "week_number"] = bi_brand_metrics_daily["sample_date"].dt.isocalendar().week

    scaler = MinMaxScaler()
    trend_cols = ["products", "avg_reviews_score", "avg_reviews_count", "daily_items", "daily_items_share",
                  "daily_sales",
                  "daily_sales_share"]

    logger.info("Scaling bi_brand_metrics_daily dataset for trend analysis")
    for col in trend_cols:
        bi_brand_metrics_daily["scaled_" + col] = bi_brand_metrics_daily.groupby(by=["brand", "category_id"])[
            col].transform(scale_data, scaler)
    logger.info("Scaling completed for bi_brand_metrics_daily")

    for col in trend_cols:
        logger.info("Evaluating trend analysis score for " + col)
        trend_col_score = compute_trend_score(bi_brand_metrics_daily, col, "category_id", brand_list)
        brands = pd.merge(brands, trend_col_score, on="brand", how="left",
                          validate="one_to_one")
        print("Computed trend analysis score for " + col)
        logger.info("Computed trend analysis score for " + col)

    del bi_brand_metrics_daily

    logger.info("Evaluating trend analysis score for pricing")
    product_brands_daily = pd.read_csv("./datasets/product_brands_daily.csv")

    # Converting the sample_date to datetime
    product_brands_daily.loc[:, "sample_date"] = pd.to_datetime(product_brands_daily["sample_date"])
    product_brands_daily.loc[:, "week_number"] = product_brands_daily["sample_date"].dt.isocalendar().week

    logger.info("Scaling product_brands_daily dataset for trend analysis")
    product_brands_daily["scaled_new_price"] = product_brands_daily.groupby(by=["brand", "product_id"])[
        "new_price"].transform(scale_data, scaler)
    logger.info("Scaling completed for product_brands_daily")

    trend_col_score = compute_trend_score(product_brands_daily, "new_price", "product_id", brand_list)
    logger.info("Evaluating trend analysis score for new_price")
    del product_brands_daily
    brands = pd.merge(brands, trend_col_score, on="brand", how="left", validate="one_to_one")
    logger.info("Computed trend analysis score for new_price")
    logger.info("Completed evaluation of trend metrics")
    return brands
