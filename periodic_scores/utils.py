import numpy as np
import pandas as pd
import scipy.stats as sc
from sklearn.preprocessing import MinMaxScaler
from trend_scores.utils import category_weight


# Function to calculate entropy
def calculate_entropy(x, agg_field, handle_single_brand=True):
    if handle_single_brand:
        # Maximum entropy if the sum is 0 or if the brand has only one data point
        if x[agg_field].sum() != 0 and len(x[agg_field]) > 1:
            return sc.entropy(x[agg_field] / x[agg_field].sum())
    else:
        # Maximum entropy if the sum is 0
        if x[agg_field].sum() != 0:
            return sc.entropy(x[agg_field] / x[agg_field].sum())
    return 1000


def get_competition_entropy(df, field_name, agg_field):

    brand_agg_field = df.groupby(by=["brand", agg_field])[field_name].mean().reset_index()

    # Function to calculate entropy
    # calculate_entropy = lambda x: sc.entropy(x["daily_sales"] / x["daily_sales"].sum())

    # Calculating entropy for each brand based on daily_sales
    cat_entropy = brand_agg_field.groupby(by="category_id").apply(calculate_entropy, field_name, False).reset_index()
    cat_entropy.rename(columns={0: "category_entropy"}, inplace=True)
    scaler = MinMaxScaler()
    cat_entropy["category_entropy"] = scaler.fit_transform(cat_entropy[["category_entropy"]])
    cat_entropy["category_entropy"] = cat_entropy["category_entropy"].map(lambda x: 1 - x)
    category_sales_weightage = category_weight(df, field_name, agg_field)
    cat_entropy = pd.merge(category_sales_weightage, cat_entropy, on="category_id", how="inner")
    cat_entropy["competition_entropy"] = cat_entropy["category_entropy"] * cat_entropy["weightage"]
    cat_entropy = cat_entropy.groupby(by="brand")["competition_entropy"].sum().reset_index()
    cat_entropy.rename(columns={"competition_entropy": "periodic_competition_entropy"}, inplace=True)
    return cat_entropy


def get_out_of_pct_score(products, products_snapshot):
    # out_of_stock for the whole time frame.
    # There are null values in the joined table, but it is removed while calculating the mean out_of_stock_pct
    out_of_stocks = pd.merge(products_snapshot, products, on="product_id", how="inner", validate="one_to_one").loc[:, [
                                                                                                  "product_id",
                                                                                                  "brand",
                                                                                                  "out_of_stock_pct"]]

    # Replacing -1 with NaN since -1 indicate missing value
    out_of_stocks["out_of_stock_pct"].replace(-1, np.nan, inplace=True)

    # Null values in out_of_stock_pct is ignored while computing the mean
    out_of_stock_score = out_of_stocks.groupby(by="brand")["out_of_stock_pct"].mean().reset_index()

    # Rescaling so that 0% out_of_stock_pct has the highest score and 100% out_of_stock_pct has the lowest score
    out_of_stock_score["periodic_out_of_stock"] = out_of_stock_score["out_of_stock_pct"].map(lambda x: (100 - x) / 100)
    out_of_stock_score.drop(columns=["out_of_stock_pct"], inplace=True)
    return out_of_stock_score


def get_ratio_to_leader(df, field_name, agg_field):
    # Finding the top brand sales in each category
    ratio_to_leader = df.groupby(by=["brand", agg_field])[field_name].mean().reset_index()
    ratio_to_leader["max_"+field_name] = ratio_to_leader.groupby(by="category_id")[field_name].transform(max)
    ratio_to_leader["ratio_to_leader"] = ratio_to_leader[field_name] / ratio_to_leader["max_"+field_name]
    category_sales_weights = category_weight(ratio_to_leader, field_name, agg_field)
    ratio_to_leader = pd.merge(ratio_to_leader, category_sales_weights, on=["brand", "category_id"], how="inner")
    ratio_to_leader["weighted_ratio"] = ratio_to_leader["ratio_to_leader"] * ratio_to_leader["weightage"]
    ratio_to_leader = ratio_to_leader.groupby(by="brand")["weighted_ratio"].sum().reset_index()
    ratio_to_leader.rename(columns={"weighted_ratio": "periodic_ratio_to_leader"}, inplace=True)
    return ratio_to_leader


def get_number_concentration(df, field_name):
    # Compute a score based on the number of products each brand has
    # First we find the mean number of products across the time frame for each brand
    # Then it is represented as a fraction of the median of mean number of products
    df_con = (df.groupby(by="brand")[field_name].mean() / df.groupby(by="brand")[field_name].mean().median(
    )).reset_index()
    df_con.rename(columns={field_name: field_name + "_con"}, inplace=True)
    return df_con


def get_revenue_concentration(df, field_name, agg_field):
    # Entropy is used to compare the uniformity of the revenue concentration of brands
    # Entropy is lowest if the distribution is uniform while it is maximum if it s concentrated on few data points
    # Drawback - Even a brand with just one data point will have low entropy (since it is always uniformly distributed)

    brand_products = df.groupby(by=["brand", agg_field])[field_name].sum().reset_index()

    # Calculating entropy for each brand based on agg_field
    entropy = brand_products.groupby(by="brand").apply(calculate_entropy, field_name).reset_index()

    # Finding the brand with maximum entropy (other than 1000)
    max_entropy = entropy.loc[entropy[0] != 1000, 0].max()

    # Replacing the entropy of 1000 with the second maximum value
    entropy[0].replace(1000, max_entropy, inplace=True)
    # Rescaling the entropy score is reversed
    # i.e. Uniform distribution has the highest score and concentrated distribution has the lowest score
    scaler = MinMaxScaler()
    entropy[agg_field+"_"+field_name+"_con"] = scaler.fit_transform(entropy[[0]])
    entropy[agg_field+"_"+field_name+"_con"] = entropy[agg_field+"_"+field_name+"_con"].map(lambda x: 1 - x)
    # print(entropy)
    entropy.drop(columns=[0], inplace=True)
    return entropy
