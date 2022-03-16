import logging
import pandas as pd
from periodic_scores.utils import get_out_of_pct_score, get_ratio_to_leader, get_number_concentration, \
    get_revenue_concentration, get_competition_entropy

logger = logging.getLogger()


def compute_periodic_scores(brands):
    
    bi_brand_metrics_daily = pd.read_csv("./datasets/bi_brand_metrics_daily.csv")    

    logger.info("Evaluating competition_entropy score")
    competition_entropy = get_competition_entropy(bi_brand_metrics_daily, "daily_sales", "category_id")
    brands = pd.merge(brands, competition_entropy, on="brand", how="left", validate="one_to_one")
    del competition_entropy
    logger.info("Computed competition_entropy score")

    logger.info("Evaluating ratio_to_leader score")
    ratio_to_leader = get_ratio_to_leader(bi_brand_metrics_daily, "daily_sales", "category_id")
    brands = pd.merge(brands, ratio_to_leader, on="brand", how="left", validate="one_to_one")
    del ratio_to_leader
    logger.info("Computed ratio_to_leader score")

    logger.info("Evaluating product_concentration score")
    product_con = get_number_concentration(bi_brand_metrics_daily, "products")
    
    logger.info("Evaluating category_concentration score")
    brand_category = bi_brand_metrics_daily.groupby(by=["brand"]).nunique().reset_index()
    category_revenue_con = get_revenue_concentration(bi_brand_metrics_daily, "daily_sales_share", "category_id")
    
    del bi_brand_metrics_daily
    category_con = get_number_concentration(brand_category, "category_id")
    category_total_con = pd.merge(category_con, category_revenue_con, on="brand", how="inner", validate="one_to_one")
    
    del category_con
    category_total_con["periodic_category_concentration"] = category_total_con["category_id_con"] * category_total_con[
                                                                    "category_id_daily_sales_share_con"]
    brands = pd.merge(brands, category_total_con.loc[:, ["brand", "periodic_category_concentration"]], on="brand",
                      how="left", validate="one_to_one")
    del category_total_con
    logger.info("Computed category_concentration score")
    
    
    bi_product_metrics_daily = pd.read_csv("./datasets/bi_product_metrics_daily.csv")
    products = pd.read_csv("./datasets/products.csv")
    
    product_brand_daily = pd.merge(bi_product_metrics_daily, products, on="product_id", how="inner",
                                   validate="many_to_one")
    
    del bi_product_metrics_daily
    del products
    
    product_revenue_con = get_revenue_concentration(product_brand_daily, "daily_sales", "product_id")
    product_total_con = pd.merge(product_con, product_revenue_con, on="brand", how="inner", validate="one_to_one")
    del product_con
    del product_revenue_con
    product_total_con["periodic_product_concentration"] = product_total_con["products_con"] * product_total_con[
                                                            "product_id_daily_sales_con"]
    brands = pd.merge(brands, product_total_con.loc[:, ["brand", "periodic_product_concentration"]], on="brand",
                      how="left", validate="one_to_one")
    del product_total_con
    logger.info("Computed product_concentration score")
    
    
    logger.info("Evaluating out_of_stock score")
    products = pd.read_csv("./datasets/products.csv")
    product_snapshots = pd.read_csv("./datasets/product_snapshots.csv")
    out_of_stock = get_out_of_pct_score(products, product_snapshots)
    del products
    del product_snapshots
    brands = pd.merge(brands, out_of_stock, on="brand", how="left", validate="one_to_one")
    del out_of_stock
    logger.info("Computed out_of_stock score")
    
    logger.info("Completed evaluation of periodic metrics")
    return brands
