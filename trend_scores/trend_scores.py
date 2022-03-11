import logging
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from trend_scores.utils import scale_data, compute_trend_score

logger = logging.getLogger()


def compute_trend_scores(bi_brand_metrics_daily, bi_product_metrics_daily, products, brands):

    product_brand_daily = pd.merge(bi_product_metrics_daily, products, on="product_id", how="inner",
                                   validate="many_to_one")

    # Converting the sample_date to datetime
    bi_brand_metrics_daily.loc[:, "sample_date"] = pd.to_datetime(bi_brand_metrics_daily["sample_date"])
    bi_brand_metrics_daily.loc[:, "week_number"] = bi_brand_metrics_daily["sample_date"].dt.isocalendar().week

    product_brand_daily.loc[:, "sample_date"] = pd.to_datetime(product_brand_daily["sample_date"])
    product_brand_daily.loc[:, "week_number"] = product_brand_daily["sample_date"].dt.isocalendar().week

    scaler = MinMaxScaler()
    trend_cols = ["products", "avg_reviews_score", "avg_reviews_count", "daily_items", "daily_items_share",
                  "daily_sales",
                  "daily_sales_share"]

    logger.info("Scaling dataset for trend analysis")
    for col in trend_cols:
        bi_brand_metrics_daily["scaled_" + col] = bi_brand_metrics_daily.groupby(by=["brand", "category_id"])[
            col].transform(scale_data, scaler)
    product_brand_daily["scaled_new_price"] = product_brand_daily.groupby(by=["brand", "product_id"])[
        "new_price"].transform(scale_data, scaler)
    logger.info("Scaling completed")

    for col in trend_cols:
        logger.info("Evaluating trend analysis score for " + col)
        trend_col_score = compute_trend_score(bi_brand_metrics_daily, col, "category_id")
        brands = pd.merge(brands, trend_col_score, on="brand", how="left",
                          validate="one_to_one")
        logger.info("Computed trend analysis score for " + col)

    logger.info("Evaluating trend analysis score for pricing")
    trend_col_score = compute_trend_score(product_brand_daily, "new_price", "product_id")
    brands = pd.merge(brands, trend_col_score, on="brand", how="left", validate="one_to_one")
    logger.info("Computed trend analysis score for pricing")
    logger.info("Completed evaluation of trend metrics")
    return brands
