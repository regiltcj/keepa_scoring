import pandas as pd


def get_number_of_sellers(products, product_offer_snapshots, bi_product_metrics_daily):
    # Combining product_offer_snapshots and products table
    product_sellers = pd.merge(product_offer_snapshots, products, on="product_id", how="inner", validate="many_to_one")
    product_sellers = product_sellers.loc[:, ["product_id", "brand", "seller_id"]]
    # Finding the count of sellers for each product_id and brand
    product_sellers = product_sellers.groupby(by=["product_id", "brand"])["seller_id"].count().reset_index()

    # Finding the average items sold for each product
    product_avg_sales = bi_product_metrics_daily.groupby(by="product_id")["daily_items"].mean().reset_index()
    # Combining product_sellers and product_avg table
    product_sellers = pd.merge(product_sellers, product_avg_sales, on="product_id", how="inner", validate="many_to_one")
    # Finding the weights for each product based on its sales contribution to the brand
    product_sellers["weightage"] = product_sellers.groupby(by="brand")["daily_items"].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 0)
    # Calculating weighted sum of the sellers
    product_sellers["weighted_sellers"] = product_sellers["seller_id"] * product_sellers["weightage"]
    number_of_sellers = product_sellers.groupby(by="brand").apply(
        lambda x: x["weighted_sellers"].sum() if x["weighted_sellers"].sum() > 0 else x[
            "seller_id"].sum()).reset_index()
    number_of_sellers.rename(columns={0: "sellers"}, inplace=True)
    return number_of_sellers


def get_price_ratio(df):
    if df["recent_price"].max() != 0:
        return df["recent_price"].min() / df["recent_price"].max()
    else:
        return 0


def get_snapshot_pricing(products, bi_product_metrics_daily, product_offer_snapshots):
    # Combining product_offer_snapshots and products table
    non_null_price_products = product_offer_snapshots.loc[product_offer_snapshots["recent_price"] != -1]
    product_sellers = pd.merge(non_null_price_products, products, on="product_id", how="inner", validate="many_to_one")
    product_sellers = product_sellers.loc[:, ["product_id", "brand", "recent_price"]]
    # Finding min to max price ratio
    product_sellers = product_sellers.groupby(by=["product_id", "brand"]).apply(get_price_ratio).reset_index()

    # Finding the average sales for each product
    product_avg_sales = bi_product_metrics_daily.groupby(by="product_id")["daily_sales"].mean().reset_index()
    # Combining product_sellers and product_avg table
    product_sellers = pd.merge(product_sellers, product_avg_sales, on="product_id", how="inner", validate="many_to_one")

    # Finding the weights for each product based on its sales contribution to the brand
    product_sellers["weightage"] = product_sellers.groupby(by="brand")["daily_sales"].transform(
        lambda x: x / x.sum() if x.sum() > 0 else 0)
    # Calculating weighted sum of the price_ratio
    product_sellers["snapshot_pricing"] = product_sellers[0] * product_sellers["weightage"]
    price_score = product_sellers.groupby(by="brand")["snapshot_pricing"].sum().reset_index()
    return price_score
